-- Repeatable CloudTMS function/view authority: candidate_provisional_expense_carrier_lifecycle
-- Use CREATE OR REPLACE and preserve owner, security, search_path, and ACL contracts.

\set ON_ERROR_STOP on

begin;

-- Provenance, not inherited Client routing, identifies a provisional expense
-- carrier. Never classify the root, an additional-hours row or source lineage
-- as an expense reservation merely because its current financial total is zero.
create or replace function private._candidate_provisional_expense_carrier_v1(
  p_contract_week_id uuid
)
returns boolean
language sql
stable
security definer
set search_path = pg_catalog, public, private, pg_temp
as $function$
  select exists (
    select 1
    from public.contract_weeks week_row
    join public.contracts contract_row on contract_row.id=week_row.contract_id
    where week_row.id=p_contract_week_id
      and week_row.timesheet_id is null
      and week_row.additional_seq>0 and week_row.is_adjustment
      and week_row.status='OPEN'
      and coalesce(week_row.day_entries_json,'[]'::jsonb)='[]'::jsonb
      and coalesce(week_row.planned_schedule_json,'[]'::jsonb)='[]'::jsonb
      and not exists (
        select 1 from jsonb_path_query(
          coalesce(week_row.totals_json,'{}'::jsonb),
          '$.** ? (@.type() == "number")'
        ) number_value
        where (number_value #>> '{}')::numeric<>0
      )
      and not exists (
        select 1 from public.weekly_source_row_timesheet_lineages lineage
        where lineage.contract_week_id=week_row.id
      )
      and not exists (
        select 1 from public.candidate_submission_workflows workflow
        where workflow.contract_week_id=week_row.id
          and (workflow.workflow_kind<>'CONTRACT_EXPENSE'
            or workflow.candidate_id is distinct from contract_row.candidate_id
            or workflow.contract_id is distinct from week_row.contract_id
            or workflow.week_ending_date is distinct from week_row.week_ending_date
            or workflow.target_timesheet_id is not null)
      )
      and exists (
        select 1 from public.audit_events event
        where event.object_type='contract_week'
          and event.object_id_text=week_row.id::text
          and event.action='CANDIDATE_EXPENSE_CARRIER_CREATED'
          and event.after_json->>'contract_id'=week_row.contract_id::text
          and event.after_json->>'week_ending_date'=week_row.week_ending_date::text
      )
  );
$function$;

alter function private._candidate_provisional_expense_carrier_v1(uuid) owner to postgres;
revoke all on function private._candidate_provisional_expense_carrier_v1(uuid)
  from public,anon,authenticated,service_role;

create or replace function private._candidate_route_family_v1(
  p_timesheet_id uuid default null,
  p_contract_week_id uuid default null
)
returns jsonb
language plpgsql
stable
security definer
set search_path = pg_catalog, public, private, pg_temp
as $function$
declare
  v_week public.contract_weeks%rowtype;
  v_contract public.contracts%rowtype;
  v_timesheet public.timesheets%rowtype;
  v_fin public.timesheets_financials%rowtype;
  v_policy jsonb;
  v_authority jsonb;
  v_effective_mode public.submission_mode_enum;
  v_import boolean:=false;
  v_config_import boolean:=false;
  v_snapshot_import boolean:=false;
  v_has_external_source_rows boolean:=false;
  v_route_import boolean:=false;
  v_route_type text:='';
  v_route_no_timesheet_required boolean:=false;
  v_import_source text:='NONE';
  v_qr_backed boolean:=false;
  v_family text;
  v_paper_fallback boolean:=false;
  v_is_daily boolean:=false;
  v_is_adjustment boolean:=false;
  v_basis text:='';
  v_is_nhsp boolean:=false;
  v_autoprocess_hr boolean:=false;
  v_provisional_expense_carrier boolean:=false;
begin
  if p_timesheet_id is null and p_contract_week_id is null then
    raise exception 'CANDIDATE_RECORD_IDENTITY_REQUIRED' using errcode='22023';
  end if;

  if p_timesheet_id is not null then
    select * into v_timesheet
    from public.timesheets
    where timesheet_id=p_timesheet_id;
    if not found then
      raise exception 'CANDIDATE_TIMESHEET_NOT_FOUND' using errcode='P0002';
    end if;
  end if;

  if p_contract_week_id is not null then
    select * into v_week
    from public.contract_weeks
    where id=p_contract_week_id;
  else
    select week_row.* into v_week
    from public.contract_weeks week_row
    where week_row.timesheet_id=p_timesheet_id
    order by week_row.updated_at desc,week_row.id desc
    limit 1;
  end if;

  if v_week.id is null then
    if v_timesheet.timesheet_id is null
       or v_timesheet.sheet_scope<>'DAILY'::public.timesheet_scope_enum
       or v_timesheet.contract_id is null then
      raise exception 'CANDIDATE_CONTRACT_WEEK_NOT_FOUND' using errcode='P0002';
    end if;
    select * into v_contract
    from public.contracts
    where id=v_timesheet.contract_id;
  else
    select * into v_contract
    from public.contracts
    where id=v_week.contract_id;
  end if;
  if not found then
    raise exception 'CANDIDATE_CONTRACT_NOT_FOUND' using errcode='P0002';
  end if;

  if v_timesheet.timesheet_id is null and v_week.timesheet_id is not null then
    select * into v_timesheet
    from public.timesheets
    where timesheet_id=v_week.timesheet_id;
  end if;

  if v_timesheet.timesheet_id is not null then
    select * into v_fin
    from public.timesheets_financials financial_row
    where financial_row.timesheet_id=v_timesheet.timesheet_id
      and financial_row.is_current=true
    order by financial_row.computed_at_utc desc nulls last,
      financial_row.updated_at desc,financial_row.id desc
    limit 1;
  end if;

  v_policy:=private._candidate_policy_resolve_v1(
    v_contract.client_id,v_contract.id,
    coalesce(
      v_week.week_ending_date,v_timesheet.week_ending_date,
      private._candidate_daily_work_date_v1(
        v_timesheet.worked_start_iso,v_timesheet.scheduled_start_iso,null
      )
    )
  );

  v_effective_mode:=case
    when v_timesheet.submission_mode='ELECTRONIC'::public.submission_mode_enum
      or v_timesheet.candidate_submission_route_intent='ELECTRONIC'
      or v_week.submission_mode_snapshot='ELECTRONIC'::public.submission_mode_enum
      then 'ELECTRONIC'::public.submission_mode_enum
    else coalesce(
      v_timesheet.submission_mode,
      v_week.submission_mode_snapshot,
      private._candidate_submission_mode_v1(
        v_contract.client_id,v_contract.id,
        coalesce(
          v_week.week_ending_date,v_timesheet.week_ending_date,
          private._candidate_daily_work_date_v1(
            v_timesheet.worked_start_iso,v_timesheet.scheduled_start_iso,null
          )
        )
      )
    )
  end;

  v_is_daily:=v_timesheet.timesheet_id is not null
    and v_timesheet.sheet_scope='DAILY'::public.timesheet_scope_enum;

  if v_is_daily then
    -- Daily has no Contract Week snapshot and retains the existing resolver
    -- unchanged. The performance regression is the Weekly fan-out path.
    v_authority:=private._candidate_import_authoritative_v1(
      v_contract.client_id,v_contract.id,v_timesheet.timesheet_id,
      to_jsonb(v_fin),
      private._candidate_daily_work_date_v1(
        v_timesheet.worked_start_iso,v_timesheet.scheduled_start_iso,
        v_timesheet.week_ending_date
      )
    );
    v_import:=coalesce((v_authority->>'is_import_authoritative')::boolean,false);
    v_import_source:=coalesce(v_authority->>'source_family','NONE');
  else
    v_authority:=private._contract_settings_effective_core_v1(
      v_contract.client_id,v_contract.id,
      coalesce(v_week.week_ending_date,v_timesheet.week_ending_date),
      'WEEKLY',v_timesheet.timesheet_id
    );
    v_config_import:=coalesce(
      (v_authority#>>'{applicability,import_authoritative}')::boolean,false
    );
    v_is_nhsp:=coalesce((v_authority#>>'{values,is_nhsp}')::boolean,false);
    v_autoprocess_hr:=coalesce(
      (v_authority#>>'{values,autoprocess_hr}')::boolean,false
    );
    v_route_no_timesheet_required:=coalesce(
      (v_authority#>>'{values,no_timesheet_required}')::boolean,false
    );
    v_basis:=upper(coalesce(v_fin.basis::text,''));
    v_has_external_source_rows:=case
      when jsonb_typeof(v_fin.external_source_rows_json)='array'
        then jsonb_array_length(v_fin.external_source_rows_json)>0
      when jsonb_typeof(v_fin.external_source_rows_json)='object'
        then v_fin.external_source_rows_json<>'{}'::jsonb
      else false
    end;
    v_snapshot_import:=v_fin.nhsp_import_id is not null
      or v_has_external_source_rows
      or v_basis in (
        'NHSP','NHSP_ADJUSTMENT','HEALTHROSTER_SELF_BILL',
        'HEALTHROSTER_SELF_BILL_ADJUSTMENT','HEALTHROSTER_ADJUSTMENT',
        'HEALTHROSTER_WEEKLY','HEALTHROSTER_WEEKLY_ADJUSTMENT'
      );
    v_is_adjustment:=coalesce(v_timesheet.is_adjustment,false)
      or coalesce(v_week.is_adjustment,false)
      or coalesce(v_week.additional_seq,0)>0
      or v_timesheet.parent_timesheet_id is not null
      or v_timesheet.correction_id is not null
      or v_timesheet.correction_kind is not null;

    v_route_type:=case
      when v_is_adjustment and (
        v_basis in ('NHSP','NHSP_ADJUSTMENT') or v_is_nhsp
      ) then 'WEEKLY_NHSP_ADJUSTMENT'
      when v_is_adjustment and (
        v_basis in ('HEALTHROSTER_ADJUSTMENT','HEALTHROSTER_SELF_BILL')
        or v_autoprocess_hr
      ) then 'WEEKLY_HEALTHROSTER_ADJUSTMENT'
      when v_is_adjustment then 'WEEKLY_MANUAL_ADJUSTMENT'
      when v_basis='NHSP_ADJUSTMENT' then 'WEEKLY_NHSP_ADJUSTMENT'
      when v_basis='HEALTHROSTER_ADJUSTMENT' then 'WEEKLY_HEALTHROSTER_ADJUSTMENT'
      when v_basis='NHSP' or v_is_nhsp then 'WEEKLY_NHSP'
      when v_autoprocess_hr or v_basis='HEALTHROSTER_SELF_BILL'
        then 'WEEKLY_HEALTHROSTER'
      when v_effective_mode='ELECTRONIC'::public.submission_mode_enum
        then 'WEEKLY_ELECTRONIC'
      when v_effective_mode='MANUAL'::public.submission_mode_enum
        then 'WEEKLY_MANUAL'
      else 'UNKNOWN'
    end;
    v_route_import:=v_route_type in ('WEEKLY_NHSP','WEEKLY_NHSP_ADJUSTMENT')
      or (
        v_route_type='WEEKLY_HEALTHROSTER'
        and v_route_no_timesheet_required
      );
    v_import:=v_config_import or v_route_import or v_snapshot_import;
    v_import_source:=case
      when v_config_import then 'CONFIG_'||coalesce(
        v_authority->>'configured_route','IMPORT_AUTHORITATIVE'
      )
      when v_route_import then 'ROUTE_'||v_route_type
      when v_fin.nhsp_import_id is not null then 'NHSP_IMPORT_SNAPSHOT'
      when v_has_external_source_rows then 'EXTERNAL_SOURCE_SNAPSHOT'
      when v_snapshot_import then 'IMPORT_BASIS_SNAPSHOT'
      else 'NONE'
    end;
  end if;

  v_provisional_expense_carrier:=private._candidate_provisional_expense_carrier_v1(v_week.id);
  if v_provisional_expense_carrier then
    -- This reservation has no imported hours. Its Client's import authority
    -- continues to govern the separate hours anchor, never this expense row.
    v_import:=false;
    v_import_source:='NONE';
  end if;

  v_qr_backed:=v_timesheet.qr_status is not null
    or v_timesheet.qr_token is not null
    or v_timesheet.qr_r2_key is not null
    or exists(
      select 1
      from public.candidate_submission_workflows workflow
      where workflow.target_timesheet_id=v_timesheet.timesheet_id
        and workflow.route='PAPER'
        and workflow.state not in (
          'CANCELLED','REJECTED','REFUSED','EXPIRED','SUPERSEDED'
        )
    );
  v_paper_fallback:=not v_is_daily
    and coalesce((v_policy->>'paper_submission_enabled')::boolean,false);
  v_family:=case
    when v_import then 'IMPORT_AUTHORITATIVE'
    when v_qr_backed then 'QR'
    when v_effective_mode='ELECTRONIC' then 'ELECTRONIC'
    when v_timesheet.timesheet_id is null and v_paper_fallback then 'QR'
    else 'MANUAL_NON_QR'
  end;

  return jsonb_build_object(
    'route_family',v_family,
    'effective_submission_mode',v_effective_mode,
    'pending_route_intent',v_timesheet.candidate_submission_route_intent,
    'import_authoritative',v_import,
    'import_source_family',v_import_source,
    'qr_backed',v_qr_backed,
    'electronic_paper_fallback_enabled',v_family='ELECTRONIC' and v_paper_fallback,
    'candidate_hours_submission_allowed',not v_provisional_expense_carrier and (
      v_family='ELECTRONIC' or (v_family='QR' and not v_is_daily)),
    'candidate_expenses_allowed',v_provisional_expense_carrier or v_family in (
      'ELECTRONIC','QR','IMPORT_AUTHORITATIVE'
    ),
    'candidate_paper_submission_allowed',not v_is_daily
      and (v_family='QR' or (v_family='ELECTRONIC' and v_paper_fallback)),
    'candidate_no_work_allowed',not v_provisional_expense_carrier and (
      v_family='ELECTRONIC' or (v_family='QR' and not v_is_daily)),
    'policy',v_policy
  );
end;
$function$;

alter function private._candidate_route_family_v1(uuid,uuid) owner to postgres;
revoke all on function private._candidate_route_family_v1(uuid,uuid)
  from public,anon,authenticated,service_role;

comment on function private._candidate_route_family_v1(uuid,uuid) is
  'Candidate route authority. Weekly reads reuse loaded canonical facts and never recursively query the complete Office Timesheet Summary view; Daily behaviour is unchanged.';


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
           t.qr_r2_key,t.qr_signed_hash,t.qr_signed_at_utc,
           t.additional_units_week,t.additional_units_per_day,
           tf.additional_units_json,tf.total_hours,tf.total_pay_ex_vat,tf.total_charge_ex_vat,
           tf.nhsp_import_id,tf.external_source_rows_json,
           tf.processing_status,tf.authorised_at_utc,
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
    where private._candidate_provisional_expense_carrier_v1(expense_row.id)
       or expense_row.capabilities->>'record_role'='EXPENSE_ONLY'
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
        and (
          workflow.target_timesheet_id=carrier.timesheet_id
          or (
            workflow.target_timesheet_id is null
            and workflow.contract_week_id=carrier.id
            and not exists (
              select 1
              from public.candidate_submission_workflows exact_owner
              where exact_owner.candidate_id=v_candidate_id
                and exact_owner.contract_id=carrier.contract_id
                and exact_owner.week_ending_date=carrier.week_ending_date
                and exact_owner.target_timesheet_id=carrier.timesheet_id
                and exact_owner.state not in ('CANCELLED','SUPERSEDED','REJECTED')
            )
          )
        )
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
        coalesce(
          nullif(resolved.immutable_submission_json#>>'{canonical_tsfin_snapshot,total_hours}','')::numeric,
          nullif(resolved.immutable_submission_json#>>'{hours_submission,canonical_tsfin_snapshot,total_hours}','')::numeric
        )
        order by resolved.updated_at_utc desc,resolved.id
      ) filter (where resolved.state in (
        'WORKER_SUBMITTED','WORKER_SUBMITTED_PENDING_REVIEW_DOCUMENT','READY_FOR_MANAGER_APPROVAL',
        'AWAITING_MANAGER_APPROVAL','MANAGER_APPROVED','MANAGER_APPROVED_PENDING_FINAL_DOCUMENT',
        'READY_TO_FINALISE','AWAITING_PAPER_RETURN','RECEIVED'
      ) and coalesce(
        nullif(resolved.immutable_submission_json#>>'{canonical_tsfin_snapshot,total_hours}',''),
        nullif(resolved.immutable_submission_json#>>'{hours_submission,canonical_tsfin_snapshot,total_hours}','')
      ) is not null))[1]
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
      -- A submitted Weekly workflow can already own a Timesheet identity while
      -- its financial row still carries the empty pre-finalisation value. Show
      -- the immutable submitted hours only for that empty gap; a populated
      -- current financial value remains authoritative after processing.
      coalesce(nullif(base.total_hours,0),workflows.submitted_total_hours,base.total_hours,0)
        as overlay_total_hours,
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
          or (
            upper(coalesce(page.line_type::text,''))='HOURS'
            and page.total_pay_ex_vat is not null
            and page.total_charge_ex_vat is not null
            and page.expenses_pay_ex_vat is not null
            and page.expenses_charge_ex_vat is not null
            and page.mileage_pay_ex_vat is not null
            and page.mileage_charge_ex_vat is not null
            and page.total_pay_ex_vat=page.expenses_pay_ex_vat+page.mileage_pay_ex_vat
            and page.total_charge_ex_vat=page.expenses_charge_ex_vat+page.mileage_charge_ex_vat
            and abs(page.expenses_pay_ex_vat+page.mileage_pay_ex_vat)
              +abs(page.expenses_charge_ex_vat+page.mileage_charge_ex_vat)>0::numeric
          )
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
          when page.qr_r2_key is not null or page.qr_signed_hash is not null
            or page.qr_signed_at_utc is not null then 'QR'
          when page.submission_mode='ELECTRONIC'::public.submission_mode_enum
            or page.nhsp_import_id is not null
            or coalesce(page.external_source_rows_json,'[]'::jsonb)
              not in ('[]'::jsonb,'{}'::jsonb,'null'::jsonb) then 'ELECTRONIC'
          when page.submission_mode='MANUAL'::public.submission_mode_enum
            and page.candidate_workflow_id is null then 'MANUAL'
          else 'UNKNOWN' end as expense_route_kind,
        case
          when not expense_fact.is_expense_only then null
          when expense_workflow.route='PAPER' then 'QR Expense'
          when expense_workflow.route in ('PHONE','EMAIL','ELECTRONIC') then 'Electronic Expense'
          when page.qr_r2_key is not null or page.qr_signed_hash is not null
            or page.qr_signed_at_utc is not null then 'QR Expense'
          when page.submission_mode='ELECTRONIC'::public.submission_mode_enum
            or page.nhsp_import_id is not null
            or coalesce(page.external_source_rows_json,'[]'::jsonb)
              not in ('[]'::jsonb,'{}'::jsonb,'null'::jsonb) then 'Electronic Expense'
          when page.submission_mode='MANUAL'::public.submission_mode_enum
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

create or replace function public.candidate_submission_finalize_atomic_v1(
  p_session_id uuid,
  p_environment text,
  p_workflow_id uuid,
  p_expected_generation integer,
  p_expected_row_signature text default null,
  p_idempotency_key text default null,
  p_now_utc timestamptz default now(),
  p_daily_materialisation_json jsonb default null
)
returns jsonb
language plpgsql
security definer
set search_path = pg_catalog, public, private, pg_temp
set lock_timeout = '5s'
set statement_timeout = '120s'
as $function$
declare
  v_environment text;
  v_context jsonb;
  v_candidate_id uuid;
  v_workflow public.candidate_submission_workflows%rowtype;
  v_hours_component public.candidate_submission_components%rowtype;
  v_candidate_signature public.candidate_submission_components%rowtype;
  v_manager_signature public.candidate_submission_components%rowtype;
  v_approved_request public.candidate_approval_requests%rowtype;
  v_paper_hours_return public.candidate_submission_components%rowtype;
  v_contract public.contracts%rowtype;
  v_week public.contract_weeks%rowtype;
  v_anchor_week public.contract_weeks%rowtype;
  v_anchor_timesheet public.timesheets%rowtype;
  v_daily_timesheet public.timesheets%rowtype;
  v_daily_fin public.timesheets_financials%rowtype;
  v_daily_receipt_context jsonb;
  v_daily_receipt_only boolean:=false;
  v_completion_state text:='FINALISED';
  v_completion_generation integer;
  v_current_policy jsonb;
  v_system_actor uuid;
  v_input jsonb;
  v_electronic_patch jsonb:='{}'::jsonb;
  v_render_input jsonb;
  v_result jsonb;
  v_authorise_result jsonb;
  v_expense_authorise_result jsonb;
  v_response jsonb;
  v_target_timesheet_id uuid;
  v_hours_timesheet_id uuid;
  v_expense_timesheet_id uuid;
  v_evidence_component_ids uuid[];
  v_placement jsonb;
  v_hours_result jsonb;
  v_hours_input jsonb;
  v_expense_input jsonb;
  v_effective_separation boolean:=false;
  v_after_signature text;
  v_auto_requested boolean:=false;
  v_auto_blocked boolean:=false;
  v_auto_blockers jsonb:='[]'::jsonb;
  v_constraint_name text;
  v_target_capabilities jsonb;
  v_route_authority jsonb;
  v_daily_save_input jsonb;
  v_daily_patch jsonb;
  v_daily_signature jsonb;
  v_daily_save_receipt jsonb;
  v_canonical_financials_id uuid;
  v_canonical_financial_sha256 bytea;
  v_service_finalisation jsonb;
  v_is_office_service boolean:=false;
  v_replay_probe_only boolean:=false;
  v_key_replay_probe_only boolean:=false;
  v_mutation_channel text;
  v_mutation_actor_identity text;
  v_mutation_request_hash text;
  v_mutation_receipt jsonb;
  v_prior_receipt_before jsonb;
  v_prior_receipt_after jsonb;
  v_finalisation_identity jsonb;
  v_finalisation_identity_hash text;
  v_completion_before jsonb;
  v_completion_after jsonb;
begin
  v_environment:=private._candidate_assert_environment(p_environment);
  v_service_finalisation:=coalesce(p_daily_materialisation_json->'service_finalisation','{}'::jsonb);
  v_is_office_service:=p_session_id is null
    and private._candidate_office_service_context_valid_v1(
      v_environment,nullif(v_service_finalisation->>'actor_user_id','')::uuid,'RETRY_FINALISATION'
    );
  v_replay_probe_only:=p_session_id is null
    and coalesce((v_service_finalisation->>'replay_probe_only')::boolean,false);
  v_key_replay_probe_only:=p_session_id is null
    and coalesce((v_service_finalisation->>'replay_key_probe_only')::boolean,false);
  if not v_is_office_service then
    perform private._candidate_require_feature_v1(v_environment,'candidate_app_writes');
  end if;
  if nullif(btrim(coalesce(p_idempotency_key,'')),'') is null then raise exception 'CANDIDATE_IDEMPOTENCY_KEY_REQUIRED' using errcode='22023'; end if;

  select * into v_workflow from public.candidate_submission_workflows where id=p_workflow_id for update;
  if not found or v_workflow.environment<>v_environment then
    raise exception 'CANDIDATE_WORKFLOW_NOT_FOUND' using errcode='P0002';
  end if;
  if p_session_id is null then
    v_candidate_id:=v_workflow.candidate_id;
  else
    v_context:=private._candidate_session_context_v1(p_session_id,v_environment,null,p_now_utc,true);
    v_candidate_id:=nullif(v_context->>'selected_candidate_id','')::uuid;
    if v_candidate_id is null then raise exception 'CANDIDATE_SELECTION_REQUIRED' using errcode='28000'; end if;
  end if;
  if v_workflow.candidate_id<>v_candidate_id then
    raise exception 'CANDIDATE_WORKFLOW_NOT_FOUND' using errcode='P0002';
  end if;
  if nullif(btrim(coalesce(v_workflow.idempotency_key,'')),'')=btrim(p_idempotency_key) then
    raise exception 'CANDIDATE_IDEMPOTENCY_CONFLICT'
      using errcode='40001',detail=jsonb_build_object(
        'code','CANDIDATE_IDEMPOTENCY_CONFLICT','workflow_id',v_workflow.id,
        'idempotency_key',btrim(p_idempotency_key),
        'reason','CREATION_KEY_REUSED_FOR_MUTATION'
      )::text;
  end if;
  v_mutation_channel:=case when v_is_office_service then 'OFFICE'
    when p_session_id is null then 'SERVICE' else 'CANDIDATE_CLIENT' end;
  v_mutation_actor_identity:=case when v_is_office_service
    then v_service_finalisation->>'actor_user_id' else coalesce(p_session_id::text,'SERVICE') end;
  if v_key_replay_probe_only then
    select ae.before_json,ae.after_json
    into v_prior_receipt_before,v_prior_receipt_after
    from public.audit_events ae
    where ae.object_type='candidate_workflow_mutation_receipt'
      and ae.object_id_text=v_workflow.id::text
      and ae.correlation_id=btrim(p_idempotency_key)
    order by ae.ts_utc desc,ae.id desc
    limit 1;
    if found then
      if upper(coalesce(v_prior_receipt_before->>'workflow_action',''))<>'RETRY_FINALISATION'
         or upper(coalesce(v_prior_receipt_before->>'channel',''))<>v_mutation_channel
         or coalesce(v_prior_receipt_before->>'actor_identity','')
              is distinct from coalesce(v_mutation_actor_identity,'')
         or nullif(v_prior_receipt_after->>'generation','')::integer
              is distinct from (p_expected_generation+case
                when v_workflow.workflow_kind='DAILY'
                  and v_prior_receipt_after->>'state'='RECEIVED'
                  and v_prior_receipt_after->>'office_resolution_pending'='true' then 0 else 1 end) then
        raise exception 'CANDIDATE_IDEMPOTENCY_CONFLICT'
          using errcode='40001',detail=jsonb_build_object(
            'code','CANDIDATE_IDEMPOTENCY_CONFLICT','workflow_id',v_workflow.id,
            'idempotency_key',btrim(p_idempotency_key)
          )::text;
      end if;
      return coalesce(v_prior_receipt_after,'{}'::jsonb)
        ||jsonb_build_object('idempotent_replay',true);
    end if;
    return jsonb_build_object(
      'ok',true,'replay_found',false,'workflow_id',v_workflow.id,
      'expected_generation',p_expected_generation
    );
  end if;
  -- Candidate-session calls retain their established lifecycle errors before
  -- an immutable approval identity exists. Service replay/finalisation calls
  -- continue through the receipt path below before mutable lifecycle checks.
  if p_session_id is not null then
    if v_workflow.route='PAPER' and v_workflow.state<>'RECEIVED' then
      raise exception 'CANDIDATE_PAPER_RETURN_INCOMPLETE' using errcode='55000';
    elsif v_workflow.route<>'PAPER' and v_workflow.state<>'READY_TO_FINALISE' then
      raise exception 'FINAL_SIGNED_DOCUMENT_NOT_READY' using errcode='55000';
    end if;
  end if;
  v_finalisation_identity:=v_service_finalisation->'finalisation_identity';
  if jsonb_typeof(v_finalisation_identity) is distinct from 'object' then
    if v_workflow.route='PAPER' then
      v_finalisation_identity:=jsonb_build_object(
        'contract_version','CANDIDATE_FINALISATION_IDENTITY_V1',
        'workflow_id',v_workflow.id,'workflow_generation',p_expected_generation,
        'approval_method','PAPER','approval_request_id',null,
        'approval_request_generation',null,'review_manifest_sha256_hex',null,
        'paper_return_manifest_sha256_hex',case when v_workflow.paper_return_manifest_sha256 is null
          then null else encode(v_workflow.paper_return_manifest_sha256,'hex') end
      );
    else
      select approved.* into v_approved_request
      from public.candidate_approval_requests approved
      where approved.workflow_id=v_workflow.id
        and approved.workflow_generation=p_expected_generation
        and approved.state='APPROVED'
        and (nullif(v_service_finalisation->>'approval_request_id','') is null
          or approved.id=(v_service_finalisation->>'approval_request_id')::uuid)
      order by approved.approved_at_utc desc,approved.id desc
      limit 1;
      v_finalisation_identity:=jsonb_build_object(
        'contract_version','CANDIDATE_FINALISATION_IDENTITY_V1',
        'workflow_id',v_workflow.id,'workflow_generation',p_expected_generation,
        'approval_method',coalesce(v_approved_request.method,v_service_finalisation->>'approval_method'),
        'approval_request_id',coalesce(v_approved_request.id,
          nullif(v_service_finalisation->>'approval_request_id','')::uuid),
        'approval_request_generation',v_approved_request.request_generation,
        'review_manifest_sha256_hex',case when v_approved_request.review_manifest_sha256 is null
          then null else encode(v_approved_request.review_manifest_sha256,'hex') end,
        'paper_return_manifest_sha256_hex',null
      );
    end if;
    v_service_finalisation:=v_service_finalisation||jsonb_build_object(
      'contract_version','CANDIDATE_MANAGER_FINALISATION_V1',
      'workflow_generation',p_expected_generation,
      'approval_method',v_finalisation_identity->>'approval_method',
      'approval_request_id',v_finalisation_identity->'approval_request_id',
      'approval_request_generation',v_finalisation_identity->'approval_request_generation',
      'review_manifest_sha256_hex',coalesce(v_finalisation_identity->>'review_manifest_sha256_hex',''),
      'paper_return_manifest_sha256_hex',coalesce(v_finalisation_identity->>'paper_return_manifest_sha256_hex',''),
      'finalisation_identity',v_finalisation_identity
    );
  end if;
  if jsonb_typeof(v_finalisation_identity) is distinct from 'object'
     or coalesce(v_finalisation_identity->>'contract_version','')
          <>'CANDIDATE_FINALISATION_IDENTITY_V1'
     or nullif(v_finalisation_identity->>'workflow_id','')::uuid is distinct from v_workflow.id
     or coalesce((v_finalisation_identity->>'workflow_generation')::integer,0)
          <>p_expected_generation
     or upper(coalesce(v_finalisation_identity->>'approval_method','')) not in ('EMAIL','PHONE','PAPER') then
    raise exception 'CANDIDATE_SERVICE_FINALISATION_INVALID'
      using errcode='28000',detail=jsonb_build_object('stage','IDENTITY')::text;
  end if;
  v_finalisation_identity_hash:=encode(extensions.digest(convert_to(
    v_finalisation_identity::text,'UTF8'
  ),'sha256'),'hex');
  v_mutation_request_hash:=encode(extensions.digest(convert_to(jsonb_build_object(
    'contract_version','CANDIDATE_FINALISATION_MUTATION_REQUEST_V3',
    'workflow_id',v_workflow.id,
    'action','RETRY_FINALISATION',
    'expected_generation',p_expected_generation,
    'service_finalisation',v_service_finalisation-'replay_probe_only',
    'channel',v_mutation_channel,
    'actor_identity',v_mutation_actor_identity
  )::text,'UTF8'),'sha256'),'hex');
  if v_replay_probe_only then
    if p_session_id is null and (
      coalesce(v_service_finalisation->>'contract_version','')
        <>'CANDIDATE_MANAGER_FINALISATION_V1'
       or coalesce((v_service_finalisation->>'workflow_generation')::integer,0)
         <>p_expected_generation
    ) then
      raise exception 'CANDIDATE_SERVICE_FINALISATION_INVALID'
        using errcode='28000',detail=jsonb_build_object('stage','REPLAY_ENVELOPE')::text;
    end if;
    select ae.before_json,ae.after_json
    into v_prior_receipt_before,v_prior_receipt_after
    from public.audit_events ae
    where ae.object_type='candidate_workflow_mutation_receipt'
      and ae.object_id_text=v_workflow.id::text
      and ae.correlation_id=btrim(p_idempotency_key)
    order by ae.ts_utc desc,ae.id desc
    limit 1;
    if found then
      if v_prior_receipt_before->>'request_sha256' is distinct from v_mutation_request_hash
         or upper(coalesce(v_prior_receipt_before->>'workflow_action',''))<>'RETRY_FINALISATION'
         or upper(coalesce(v_prior_receipt_before->>'channel',''))<>v_mutation_channel
         or coalesce(v_prior_receipt_before->>'actor_identity','')
              is distinct from coalesce(v_mutation_actor_identity,'')
         or nullif(v_prior_receipt_after->>'generation','')::integer
              is distinct from (p_expected_generation+case
                when v_workflow.workflow_kind='DAILY'
                  and v_prior_receipt_after->>'state'='RECEIVED'
                  and v_prior_receipt_after->>'office_resolution_pending'='true' then 0 else 1 end) then
        raise exception 'CANDIDATE_IDEMPOTENCY_CONFLICT'
          using errcode='40001',detail=jsonb_build_object(
            'code','CANDIDATE_IDEMPOTENCY_CONFLICT','workflow_id',v_workflow.id,
            'idempotency_key',btrim(p_idempotency_key)
          )::text;
      end if;
      return coalesce(v_prior_receipt_after,'{}'::jsonb)
        ||jsonb_build_object('idempotent_replay',true);
    end if;
    select ae.before_json,ae.after_json
    into v_completion_before,v_completion_after
    from public.audit_events ae
    where ae.object_type='candidate_workflow_finalisation_completion'
      and ae.object_id_text=v_workflow.id::text
      and ae.correlation_id=p_expected_generation::text||':'||v_finalisation_identity_hash
    order by ae.ts_utc desc,ae.id desc
    limit 1;
    if found then
      if v_completion_before->>'finalisation_identity_sha256'
           is distinct from v_finalisation_identity_hash
         or nullif(v_completion_after->>'generation','')::integer
           is distinct from (p_expected_generation+case
             when v_workflow.workflow_kind='DAILY'
               and v_completion_after->>'state'='RECEIVED'
               and v_completion_after->>'office_resolution_pending'='true' then 0 else 1 end) then
        raise exception 'CANDIDATE_IDEMPOTENCY_CONFLICT' using errcode='40001';
      end if;
      return coalesce(v_completion_after,'{}'::jsonb)
        ||jsonb_build_object('idempotent_replay',true);
    end if;
    return jsonb_build_object('ok',true,'replay_found',false,'workflow_id',v_workflow.id,
      'expected_generation',p_expected_generation);
  end if;
  v_mutation_receipt:=private._candidate_workflow_mutation_receipt_v1(
    v_workflow.id,p_idempotency_key,v_mutation_request_hash,'RETRY_FINALISATION',
    v_mutation_channel,v_mutation_actor_identity,
    null,p_now_utc
  );
  if coalesce((v_mutation_receipt->>'found')::boolean,false) then
    return coalesce(v_mutation_receipt->'response','{}'::jsonb)||jsonb_build_object('idempotent_replay',true);
  end if;
  select ae.before_json,ae.after_json
  into v_completion_before,v_completion_after
  from public.audit_events ae
  where ae.object_type='candidate_workflow_finalisation_completion'
    and ae.object_id_text=v_workflow.id::text
    and ae.correlation_id=p_expected_generation::text||':'||v_finalisation_identity_hash
  order by ae.ts_utc desc,ae.id desc
  limit 1;
  if found then
    return coalesce(v_completion_after,'{}'::jsonb)
      ||jsonb_build_object('idempotent_replay',true);
  end if;
  if p_session_id is null then
    if coalesce(v_service_finalisation->>'contract_version','')<>'CANDIDATE_MANAGER_FINALISATION_V1'
       or coalesce((v_service_finalisation->>'workflow_generation')::integer,0)<>v_workflow.generation
       or upper(coalesce(v_service_finalisation->>'approval_method',''))<>v_workflow.route then
      raise exception 'CANDIDATE_SERVICE_FINALISATION_INVALID'
        using errcode='28000',detail=jsonb_build_object('stage','SERVICE_ENVELOPE')::text;
    end if;
    if v_workflow.route='PAPER' then
      if nullif(v_service_finalisation->>'approval_request_id','') is not null
         or nullif(v_finalisation_identity->>'approval_request_id','') is not null
         or upper(v_finalisation_identity->>'approval_method')<>'PAPER'
         or lower(coalesce(v_finalisation_identity->>'paper_return_manifest_sha256_hex',''))
              <>encode(v_workflow.paper_return_manifest_sha256,'hex') then
        raise exception 'CANDIDATE_SERVICE_FINALISATION_INVALID'
          using errcode='28000',detail=jsonb_build_object('stage','PAPER_IDENTITY')::text;
      end if;
    else
      select * into v_approved_request
      from public.candidate_approval_requests a
      where a.id=nullif(v_service_finalisation->>'approval_request_id','')::uuid
        and a.workflow_id=v_workflow.id
        and a.workflow_generation=p_expected_generation
        and a.request_generation=coalesce(
          nullif(v_service_finalisation->>'approval_request_generation','')::integer,0
        )
        and a.method=upper(coalesce(v_service_finalisation->>'approval_method',''))
        and a.state='APPROVED'
        and encode(a.review_manifest_sha256,'hex')=lower(coalesce(v_service_finalisation->>'review_manifest_sha256_hex',''))
        and v_finalisation_identity->>'approval_request_id'=a.id::text
        and coalesce((v_finalisation_identity->>'approval_request_generation')::integer,0)=a.request_generation
        and upper(v_finalisation_identity->>'approval_method')=a.method
        and lower(coalesce(v_finalisation_identity->>'review_manifest_sha256_hex',''))
              =encode(a.review_manifest_sha256,'hex')
      for update;
      if not found then
        raise exception 'CANDIDATE_SERVICE_FINALISATION_INVALID'
          using errcode='28000',detail=jsonb_build_object('stage','APPROVAL_IDENTITY')::text;
      end if;
    end if;
  end if;
  if v_workflow.generation<>p_expected_generation then
    raise exception 'WORKFLOW_VERSION_MISMATCH'
      using errcode='40001',detail=jsonb_build_object('code','WORKFLOW_VERSION_MISMATCH','current_generation',v_workflow.generation)::text;
  end if;
  if v_workflow.workflow_kind='DAILY' then
    if v_workflow.scope<>'DAILY' or v_workflow.route not in ('PHONE','EMAIL')
       or v_workflow.contract_week_id is not null or v_workflow.week_ending_date is not null
       or v_workflow.target_timesheet_id is null
       or v_workflow.anchor_timesheet_id is distinct from v_workflow.target_timesheet_id then
      raise exception 'CANDIDATE_DAILY_IDENTITY_INVALID' using errcode='22023';
    end if;
    v_daily_receipt_context:=private._candidate_daily_receipt_context_v1(
      v_environment,v_candidate_id,v_workflow.target_timesheet_id,true,p_now_utc);
    v_daily_receipt_only:=coalesce((v_daily_receipt_context->>'candidate_first_receipt')::boolean,false)
      and coalesce((v_daily_receipt_context->>'office_resolution_pending')::boolean,false);
    select * into v_daily_timesheet
    from public.timesheets
    where timesheet_id=v_workflow.target_timesheet_id
      and is_current=true and archived_at_utc is null
      and sheet_scope='DAILY'::public.timesheet_scope_enum
      and nullif(btrim(coalesce(booking_id,'')),'') is not null
    for update;
    if not found then raise exception 'CANDIDATE_DAILY_SHIFT_NOT_FOUND' using errcode='P0002'; end if;
    if not private._candidate_daily_entitled_v1(v_candidate_id) then
      raise exception 'CANDIDATE_DAILY_ENTITLEMENT_REQUIRED' using errcode='55000';
    end if;
    select * into v_daily_fin
    from public.timesheets_financials
    where timesheet_id=v_daily_timesheet.timesheet_id
      and is_current=true and candidate_id=v_candidate_id
    order by computed_at_utc desc nulls last,updated_at desc,id desc
    limit 1
    for update;
    if (not found and not v_daily_receipt_only)
       or v_workflow.work_date is distinct from private._candidate_daily_work_date_v1(
         coalesce(v_daily_fin.worked_start_iso,v_daily_timesheet.worked_start_iso),
         v_daily_timesheet.scheduled_start_iso,
         v_daily_timesheet.week_ending_date
       ) then
      raise exception 'CANDIDATE_DAILY_SHIFT_IDENTITY_MISMATCH' using errcode='40001';
    end if;
    if v_daily_fin.authorised_at_utc is not null
       or v_daily_fin.paid_at_utc is not null
       or v_daily_fin.locked_by_invoice_id is not null
       or v_daily_timesheet.archived_at_utc is not null then
      raise exception 'CANDIDATE_RECORD_MUTATION_LOCKED' using errcode='55000';
    end if;
    if v_daily_timesheet.contract_id is not null then
      select * into v_contract
      from public.contracts
      where id=v_daily_timesheet.contract_id and candidate_id=v_candidate_id
      for update;
      if not found then raise exception 'CANDIDATE_WORKFLOW_OWNERSHIP_MISMATCH' using errcode='28000'; end if;
    end if;
    if v_daily_receipt_only then
      v_current_policy:=v_daily_receipt_context->'policy';
    else
      if coalesce(v_daily_fin.client_id,v_contract.client_id) is null then
        raise exception 'CANDIDATE_DAILY_CLIENT_NOT_FOUND' using errcode='P0002';
      end if;
      v_current_policy:=private._candidate_policy_resolve_v1(
        coalesce(v_daily_fin.client_id,v_contract.client_id),v_contract.id,v_workflow.work_date
      );
    end if;
    if (v_workflow.route='PHONE' and not coalesce((v_current_policy->>'allow_daily_manager_authorise_on_phone')::boolean,false))
       or (v_workflow.route='EMAIL' and not coalesce((v_current_policy->>'allow_daily_manager_authorise_by_email')::boolean,false)) then
      raise exception 'CANDIDATE_DAILY_APPROVAL_ROUTE_NOT_ALLOWED' using errcode='55000';
    end if;
  else
    if v_workflow.workflow_kind not in ('CONTRACT_HOURS','CONTRACT_EXPENSE','CONTRACT_COMBINED')
       or v_workflow.scope<>'WEEKLY' or v_workflow.contract_id is null
       or v_workflow.contract_week_id is null or v_workflow.week_ending_date is null then
      raise exception 'CANDIDATE_CONTRACT_WORKFLOW_IDENTITY_INVALID' using errcode='22023';
    end if;
    select * into v_contract
    from public.contracts
    where id=v_workflow.contract_id and candidate_id=v_candidate_id
    for update;
    if not found then raise exception 'CANDIDATE_WORKFLOW_OWNERSHIP_MISMATCH' using errcode='28000'; end if;
    select * into v_week
    from public.contract_weeks
    where id=v_workflow.contract_week_id
      and contract_id=v_contract.id
      and week_ending_date=v_workflow.week_ending_date
    for update;
    if not found then raise exception 'CANDIDATE_CONTRACT_WEEK_IDENTITY_MISMATCH' using errcode='40001'; end if;
    if v_workflow.anchor_timesheet_id is not null then
      select cw.* into v_anchor_week
      from public.contract_weeks cw
      join public.timesheets t on t.timesheet_id=cw.timesheet_id
        and t.is_current=true and t.archived_at_utc is null
      where cw.timesheet_id=v_workflow.anchor_timesheet_id
        and cw.contract_id=v_contract.id
        and cw.week_ending_date=v_workflow.week_ending_date;
      if not found then raise exception 'CANDIDATE_WORKFLOW_ANCHOR_MISMATCH' using errcode='40001'; end if;
    end if;
    if v_workflow.workflow_kind='CONTRACT_EXPENSE' then
      if v_workflow.anchor_timesheet_id is null
         or coalesce((private._candidate_record_capabilities_v1(
           v_workflow.anchor_timesheet_id,v_anchor_week.id,'{}'::jsonb
         )->>'hours_value')::numeric,0)<=0
            and coalesce((private._candidate_record_capabilities_v1(
              v_workflow.anchor_timesheet_id,v_anchor_week.id,'{}'::jsonb
            )->>'additional_units_value')::numeric,0)<=0 then
        -- Reuse the existing expense-admission authority for source weeks:
        -- immutable Candidate worked-hours evidence may precede source TSFIN.
        -- This neither writes nor unlocks the source-owned hours record.
        if v_workflow.anchor_timesheet_id is null
           or not coalesce((private._candidate_record_capabilities_v1(
             v_workflow.anchor_timesheet_id,v_anchor_week.id,'{}'::jsonb
           )->>'import_authoritative')::boolean,false) then
          raise exception 'NO_POSITIVE_WORKED_TIME' using errcode='55000';
        end if;
        v_placement:=public.expense_placement_resolve_v1(
          v_candidate_id,v_environment,v_workflow.anchor_timesheet_id,
          v_anchor_week.id,'{}'::jsonb,p_now_utc);
        if not coalesce((v_placement->>'ok')::boolean,false)
           or coalesce(v_placement->>'placement','') not in ('REUSE_CARRIER','CREATE_CARRIER') then
          raise exception 'CANDIDATE_EXPENSE_FINALISATION_ADMISSION_BLOCKED'
            using errcode='55000',detail=coalesce(v_placement->>'reason_code','NO_POSITIVE_WORKED_TIME');
        end if;
      end if;
    end if;
    if v_workflow.workflow_kind='CONTRACT_EXPENSE' and v_workflow.target_timesheet_id is not null then
      raise exception 'CANDIDATE_EXPENSE_TARGET_SERVER_RESOLVED' using errcode='40001';
    elsif v_workflow.workflow_kind in ('CONTRACT_HOURS','CONTRACT_COMBINED')
       and v_workflow.target_timesheet_id is distinct from v_week.timesheet_id then
      raise exception 'CANDIDATE_WORKFLOW_TARGET_MISMATCH' using errcode='40001';
    end if;
    if v_workflow.workflow_kind in ('CONTRACT_HOURS','CONTRACT_COMBINED')
       and v_week.timesheet_id is not null then
      v_target_capabilities:=private._candidate_record_capabilities_v1(
        v_week.timesheet_id,v_week.id,'{}'::jsonb
      );
      if coalesce((v_target_capabilities->>'candidate_mutation_locked')::boolean,false)
         or coalesce((v_target_capabilities->>'protected')::boolean,false)
         or not coalesce((v_target_capabilities->>'can_edit_hours')::boolean,false) then
        raise exception 'CANDIDATE_RECORD_MUTATION_LOCKED' using errcode='55000';
      end if;
    end if;
    v_route_authority:=private._candidate_route_family_v1(
      case when v_workflow.workflow_kind='CONTRACT_EXPENSE' then v_workflow.anchor_timesheet_id
        else v_week.timesheet_id end,
      case when v_workflow.workflow_kind='CONTRACT_EXPENSE' then v_anchor_week.id else v_week.id end
    );
    if v_route_authority->>'route_family'='MANUAL_NON_QR'
       or (v_route_authority->>'route_family'='IMPORT_AUTHORITATIVE'
         and v_workflow.workflow_kind<>'CONTRACT_EXPENSE')
       or (v_workflow.route='PAPER' and not coalesce((v_route_authority->>'candidate_paper_submission_allowed')::boolean,false))
       or (v_workflow.route<>'PAPER' and v_route_authority->>'route_family'='QR') then
      raise exception 'CANDIDATE_ROUTE_FAMILY_MISMATCH' using errcode='55000',detail=v_route_authority::text;
    end if;
    v_current_policy:=private._candidate_policy_resolve_v1(
      v_contract.client_id,v_contract.id,v_workflow.week_ending_date
    );
    if v_workflow.route='PAPER'
       and not coalesce((v_current_policy->>'paper_submission_enabled')::boolean,false) then
      raise exception 'CANDIDATE_PAPER_ROUTE_NOT_ALLOWED' using errcode='55000';
    end if;
  end if;
  if v_workflow.route='PAPER' then
    if v_workflow.state<>'RECEIVED'
       or v_workflow.paper_return_manifest_sha256 is null
       or private._candidate_sha256_jsonb_v1(v_workflow.paper_return_manifest_json)
          is distinct from v_workflow.paper_return_manifest_sha256
       or exists(
         select 1
         from jsonb_array_elements(v_workflow.paper_return_manifest_json->'pages') expected_page
         where (
           select count(*)
           from public.candidate_submission_components returned_page
           where returned_page.workflow_id=v_workflow.id
             and returned_page.workflow_generation=v_workflow.generation
             and returned_page.component_kind='SIGNED_RETURN'
             and returned_page.paper_return_page_key=expected_page->>'page_key'
             and returned_page.state='IMMUTABLE'
             and returned_page.source_content_sha256 is not null
         )<>1
       )
       or exists(
         select 1
         from public.candidate_submission_components returned_page
         where returned_page.workflow_id=v_workflow.id
           and returned_page.workflow_generation=v_workflow.generation
           and returned_page.component_kind='SIGNED_RETURN'
           and returned_page.state='IMMUTABLE'
           and not exists(
             select 1
             from jsonb_array_elements(v_workflow.paper_return_manifest_json->'pages') expected_page
             where expected_page->>'page_key'=returned_page.paper_return_page_key
           )
       ) then
      raise exception 'CANDIDATE_PAPER_RETURN_INCOMPLETE' using errcode='55000';
    end if;
  else
    if v_workflow.state<>'READY_TO_FINALISE' then
      raise exception 'FINAL_SIGNED_DOCUMENT_NOT_READY' using errcode='55000';
    end if;
    select * into v_approved_request
    from public.candidate_approval_requests a
      where a.workflow_id=v_workflow.id and a.workflow_generation=v_workflow.generation
        and a.state='APPROVED' and a.review_manifest_sha256=v_workflow.review_manifest_sha256
    for update;
    if not found then raise exception 'FINAL_SIGNED_DOCUMENT_NOT_READY' using errcode='55000'; end if;

    if not exists(
      select 1 from public.candidate_submission_components c
      where c.workflow_id=v_workflow.id and c.workflow_generation=v_workflow.generation
        and c.required=true and c.state<>'SUPERSEDED'
    ) or exists(
      select 1 from public.candidate_submission_components c
      where c.workflow_id=v_workflow.id and c.workflow_generation=v_workflow.generation
        and c.required=true and c.state<>'SUPERSEDED'
        and (c.state<>'IMMUTABLE' or c.review_render_state<>'READY'
          or c.final_signed_render_state<>'READY'
          or c.review_render_input_sha256 is distinct from c.final_signed_render_input_sha256)
    ) then
      raise exception 'FINAL_SIGNED_DOCUMENT_NOT_READY' using errcode='55000';
    end if;
    select * into v_hours_component from public.candidate_submission_components c
    where c.workflow_id=v_workflow.id and c.workflow_generation=v_workflow.generation
      and c.component_kind='HOURS_TIMESHEET' and c.required=true and c.state='IMMUTABLE'
    for update;
    if v_workflow.workflow_kind in ('CONTRACT_HOURS','CONTRACT_COMBINED','DAILY') and not found then
      raise exception 'FINAL_SIGNED_DOCUMENT_NOT_READY' using errcode='55000';
    elsif v_workflow.workflow_kind='CONTRACT_EXPENSE' and found then
      raise exception 'CONTRACT_EXPENSE_HOURS_COMPONENT_FORBIDDEN' using errcode='55000';
    end if;
    if v_hours_component.id is not null and v_hours_component.review_render_input_sha256
       is distinct from v_hours_component.final_signed_render_input_sha256 then
      raise exception 'FINAL_RENDER_INPUT_MISMATCH' using errcode='40001';
    end if;
    if v_workflow.workflow_kind<>'CONTRACT_EXPENSE' then
      select * into v_candidate_signature from public.candidate_submission_components c
      where c.id=v_workflow.candidate_signature_component_id and c.workflow_id=v_workflow.id
        and c.document_role='CANDIDATE_SIGNATURE' and c.state='IMMUTABLE' for update;
    elsif v_workflow.candidate_signature_component_id is not null
       or v_workflow.candidate_signature_sha256 is not null then
      raise exception 'CONTRACT_EXPENSE_CANDIDATE_SIGNATURE_FORBIDDEN' using errcode='55000';
    end if;
    select * into v_manager_signature from public.candidate_submission_components c
    where c.id=v_workflow.manager_signature_component_id and c.workflow_id=v_workflow.id
      and c.document_role='MANAGER_SIGNATURE' and c.state='IMMUTABLE'
      and c.approval_request_id=v_approved_request.id for update;
    if (v_workflow.workflow_kind<>'CONTRACT_EXPENSE' and (
          v_candidate_signature.id is null
          or v_candidate_signature.source_content_sha256 is distinct from v_workflow.candidate_signature_sha256))
       or v_manager_signature.id is null
       or v_manager_signature.source_content_sha256 is distinct from v_workflow.manager_signature_sha256
       or v_workflow.manager_approved_at_utc is null then
      raise exception 'ELECTRONIC_SIGNATURE_PAIR_INCOMPLETE' using errcode='55000';
    end if;
    v_render_input:=private._candidate_render_input_v1(v_workflow.id,v_workflow.generation);
    if v_hours_component.id is not null and decode(v_render_input->>'render_input_sha256','hex')
       is distinct from decode(
         private._candidate_component_render_input_v1(
           v_workflow.id,v_workflow.generation,v_hours_component.id
         )->>'workflow_render_input_sha256','hex'
       ) then
      raise exception 'FINAL_RENDER_INPUT_MISMATCH' using errcode='40001';
    end if;
    v_electronic_patch:=jsonb_build_object(
      'submission_mode','ELECTRONIC',
      'auth_name',v_workflow.manager_name,
      'auth_job_title',v_workflow.manager_position,
      'r2_nurse_key',v_candidate_signature.storage_key,
      'r2_auth_key',v_manager_signature.storage_key,
      'img_sha256_nurse',case when v_candidate_signature.source_content_sha256 is null then null
        else encode(v_candidate_signature.source_content_sha256,'hex') end,
      'img_sha256_auth',encode(v_manager_signature.source_content_sha256,'hex'),
      'candidate_workflow_id',v_workflow.id,
      'candidate_workflow_generation',v_workflow.generation,
      'candidate_manager_approved_at_utc',v_workflow.manager_approved_at_utc
    );
  end if;
  if v_workflow.route='PAPER' then
    v_electronic_patch:=jsonb_build_object(
      'submission_mode','MANUAL',
      'r2_nurse_key',null,
      'r2_auth_key',null,
      'candidate_workflow_id',v_workflow.id,
      'candidate_workflow_generation',v_workflow.generation,
      'candidate_manager_approved_at_utc',null
    );
  end if;

  if coalesce(v_current_policy->>'policy_fingerprint','')
     is distinct from coalesce(v_workflow.policy_snapshot_json->>'policy_fingerprint','') then
    update public.candidate_approval_requests set state='SUPERSEDED',superseded_at_utc=p_now_utc,updated_at_utc=p_now_utc
    where workflow_id=v_workflow.id and state='PENDING';
    v_response:=jsonb_build_object('ok',false,'error_code','CANDIDATE_POLICY_CHANGED','workflow_id',v_workflow.id,
      'state','SUPERSEDED','generation',v_workflow.generation+1,'current_policy',v_current_policy);
    update public.candidate_submission_workflows set state='SUPERSEDED',generation=generation+1,
      policy_snapshot_json=v_current_policy,policy_snapshot_sha256=private._candidate_sha256_jsonb_v1(v_current_policy),
      last_mutation_idempotency_key=p_idempotency_key,
      last_mutation_response_json=v_response,updated_at_utc=p_now_utc where id=v_workflow.id;
    perform private._candidate_workflow_mutation_receipt_v1(
      v_workflow.id,p_idempotency_key,v_mutation_request_hash,'RETRY_FINALISATION',
      case when v_is_office_service then 'OFFICE' when p_session_id is null then 'SERVICE' else 'CANDIDATE_CLIENT' end,
      case when v_is_office_service then v_service_finalisation->>'actor_user_id' else coalesce(p_session_id::text,'SERVICE') end,
      v_response,p_now_utc
    );
    return v_response;
  end if;

  select candidate_app_system_actor_user_id into v_system_actor from public.settings_defaults where id=1;
  if v_system_actor is null then raise exception 'CANDIDATE_SYSTEM_ACTOR_NOT_CONFIGURED' using errcode='55000'; end if;
  v_input:=v_workflow.immutable_submission_json;
  if v_input is null or private._candidate_sha256_jsonb_v1(v_input)
     is distinct from v_workflow.immutable_submission_sha256 then
    raise exception 'CANDIDATE_IMMUTABLE_SUBMISSION_MISMATCH' using errcode='40001';
  end if;
  v_workflow.issue_codes:=private._candidate_finalisation_issue_codes_v1(
    v_workflow.issue_codes,
    private._candidate_submission_issue_codes_v1(
      v_workflow.id,v_input,v_current_policy
    )
  );
  v_effective_separation:=coalesce((v_current_policy->>'expenses_require_separate_timesheet')::boolean,false);
  if v_workflow.workflow_kind in ('CONTRACT_EXPENSE','CONTRACT_COMBINED')
     and coalesce((v_current_policy->>'import_expense_separation_mandatory')::boolean,false)
     and not coalesce((v_current_policy->>'expense_invoice_email_ready')::boolean,false) then
    raise exception 'EXPENSE_INVOICE_EMAIL_REQUIRED' using errcode='55000';
  end if;

  if v_workflow.scope='WEEKLY' then
    select * into v_week from public.contract_weeks where id=v_workflow.contract_week_id for update;
    if not found then raise exception 'CANDIDATE_CONTRACT_WEEK_NOT_FOUND' using errcode='P0002'; end if;
    if v_workflow.workflow_kind='CONTRACT_COMBINED' then
      v_hours_input:=coalesce(v_input->'hours_submission',v_input);
      v_expense_input:=coalesce(v_input->'expense_submission',v_input);
      if jsonb_typeof(v_hours_input)<>'object' or jsonb_typeof(v_expense_input)<>'object' then
        raise exception 'CANDIDATE_COMBINED_SNAPSHOTS_REQUIRED' using errcode='22023';
      end if;
    elsif v_workflow.workflow_kind='CONTRACT_EXPENSE' then
      v_expense_input:=v_input;
    else
      v_hours_input:=v_input;
    end if;

    if v_hours_input is not null then
      if v_hours_input->'canonical_tsfin_snapshot' is null then
        raise exception 'CANDIDATE_CANONICAL_TSFIN_SNAPSHOT_REQUIRED' using errcode='22023';
      end if;
      perform set_config('cloudtms.candidate_electronic_finalise',v_workflow.id::text||':'||v_workflow.generation::text,true);
      v_hours_result:=public.contract_week_manual_upsert_atomic(
        p_week_id=>v_week.id,
        p_expected_timesheet_id=>v_workflow.target_timesheet_id,
        p_timesheet_create_json=>case when v_workflow.target_timesheet_id is null
          then coalesce(v_hours_input->'timesheet_create_json','{}'::jsonb)||v_electronic_patch else null end,
        p_timesheet_patch_json=>coalesce(v_hours_input->'timesheet_patch_json','{}'::jsonb)||v_electronic_patch,
        p_contract_week_patch_json=>coalesce(v_hours_input->'contract_week_patch_json','{}'::jsonb),
        p_tsfin_snapshot_json=>v_hours_input->'canonical_tsfin_snapshot',
        p_rotation_json=>null,p_actor_user_id=>v_system_actor,p_materialise_staged_evidence=>false,
        p_now_utc=>p_now_utc,p_expected_row_signature=>coalesce(p_expected_row_signature,v_workflow.expected_row_signature),
        p_queue_timesheet_materialisation_json=>jsonb_build_object('suppress_timesheet_evidence_materialisation',true)
      );
      if coalesce((v_hours_result->>'ok')::boolean,false)=false then
        raise exception 'CANDIDATE_FINALISE_CANONICAL_APPLY_FAILED' using errcode='55000',detail=v_hours_result::text;
      end if;
      v_hours_timesheet_id:=coalesce(nullif(v_hours_result->>'timesheet_id','')::uuid,
        nullif(v_hours_result#>>'{timesheet,timesheet_id}','')::uuid);
      v_after_signature:=coalesce(v_hours_result->>'row_signature',v_hours_result->>'backend_row_signature',
        v_hours_result#>>'{timesheet,row_signature}');
    end if;

    if v_expense_input is not null then
      if v_workflow.workflow_kind='CONTRACT_EXPENSE' or v_effective_separation then
        v_placement:=public.expense_carrier_resolve_or_create_atomic_v1(
          v_candidate_id,v_environment,coalesce(v_hours_timesheet_id,v_workflow.anchor_timesheet_id),
          coalesce(v_after_signature,p_expected_row_signature,v_workflow.expected_row_signature),
          p_idempotency_key||':carrier',p_now_utc);
      else
        v_placement:=jsonb_build_object(
          'placement','SAME_RECORD','target_timesheet_id',coalesce(v_hours_timesheet_id,v_workflow.target_timesheet_id),
          'target_contract_week_id',v_week.id);
      end if;
      if v_workflow.route='PAPER' then
        select array_agg(c.id order by c.component_no,c.id) into v_evidence_component_ids
        from public.candidate_submission_components c
        where c.workflow_id=v_workflow.id and c.workflow_generation=v_workflow.generation
          and c.component_kind='SIGNED_RETURN' and c.state='IMMUTABLE'
          and c.paper_return_page_key<>'HOURS_TIMESHEET';
      else
        select array_agg(c.id order by c.review_ordinal,c.id) into v_evidence_component_ids
        from public.candidate_submission_components c
        where c.workflow_id=v_workflow.id and c.workflow_generation=v_workflow.generation
          and c.required=true and c.state<>'SUPERSEDED' and c.component_kind<>'HOURS_TIMESHEET';
      end if;
      update public.candidate_submission_workflows set
        -- A first combined weekly submission has no anchor until its hours row
        -- is materialised above. Bind that worked row before expense apply so
        -- SAME_RECORD stays a combined HOURS Timesheet. A genuinely separate
        -- expense carrier still has a different target and remains EXPENSES.
        anchor_timesheet_id=case when v_workflow.workflow_kind='CONTRACT_COMBINED'
          then coalesce(anchor_timesheet_id,v_hours_timesheet_id)
          else anchor_timesheet_id end,
        contract_week_id=nullif(v_placement->>'target_contract_week_id','')::uuid,
        target_timesheet_id=nullif(v_placement->>'target_timesheet_id','')::uuid,
        updated_at_utc=p_now_utc
      where id=v_workflow.id;
      perform set_config('cloudtms.candidate_finalize_workflow',v_workflow.id::text||':'||v_workflow.generation::text,true);
      v_result:=public.timesheet_expense_apply_atomic_v1(
        v_candidate_id,v_environment,nullif(v_placement->>'target_timesheet_id','')::uuid,
        v_workflow.id,v_workflow.generation,
        case when v_placement->>'placement'='SAME_RECORD' then coalesce(v_after_signature,p_expected_row_signature,v_workflow.expected_row_signature)
          else null end,
        v_expense_input,v_evidence_component_ids,p_idempotency_key||':expense',p_now_utc);
      v_expense_timesheet_id:=nullif(v_result->>'target_timesheet_id','')::uuid;
      v_target_timesheet_id:=coalesce(v_expense_timesheet_id,v_hours_timesheet_id);
      if v_hours_result is not null then
        v_result:=jsonb_build_object('ok',true,'hours_result',v_hours_result,'expense_result',v_result,
          'hours_timesheet_id',v_hours_timesheet_id,'expense_timesheet_id',v_expense_timesheet_id);
      end if;
    else
      v_result:=v_hours_result;
      v_target_timesheet_id:=v_hours_timesheet_id;
    end if;
  else
    if not v_is_office_service then
      perform private._candidate_require_feature_v1(v_environment,'candidate_daily_finalisation');
    end if;
    v_target_timesheet_id:=v_workflow.target_timesheet_id;
    if v_target_timesheet_id is null then
      raise exception 'CANDIDATE_DAILY_TIMESHEET_REQUIRED' using errcode='22023';
    end if;
    v_daily_save_input:=private._candidate_daily_canonical_save_input_v1(
      v_workflow.id,v_workflow.generation
    );
    v_daily_patch:=v_daily_save_input->'timesheet_patch_json';
    if jsonb_typeof(p_daily_materialisation_json)<>'object' then
      raise exception 'CANDIDATE_DAILY_MATERIALISATION_REQUIRED' using errcode='22023';
    end if;
    if v_daily_receipt_only then
      if p_daily_materialisation_json->>'contract_version' is distinct from 'CANDIDATE_DAILY_FACTUAL_RECEIPT_V1'
         or p_daily_materialisation_json->>'workflow_id' is distinct from v_workflow.id::text
         or (p_daily_materialisation_json->>'workflow_generation')::integer is distinct from v_workflow.generation
         or p_daily_materialisation_json->>'timesheet_id' is distinct from v_target_timesheet_id::text then
        raise exception 'CANDIDATE_DAILY_RECEIPT_CONTEXT_CHANGED' using errcode='40001';
      end if;
      v_result:=private._candidate_daily_factual_receipt_v1(
        v_workflow.id,v_workflow.generation,
        p_daily_materialisation_json->>'canonical_save_input_sha256_hex',v_electronic_patch,p_now_utc);
      v_completion_state:='RECEIVED';
    else
    -- This private composition performs the pre-write row-signature check,
    -- factual save and bounded TSFIN write in this same finalisation transaction.
    -- Any later Process/Authorise error rolls the factual and financial write back.
    v_daily_save_receipt:=private._candidate_daily_save_recalculate_atomic_v1(
      v_workflow.id,v_workflow.generation,p_daily_materialisation_json,
      v_system_actor,p_now_utc
    );
    select * into v_daily_timesheet from public.timesheets
    where timesheet_id=v_target_timesheet_id and is_current=true for update;
    select * into v_daily_fin from public.timesheets_financials
    where id=nullif(v_daily_save_receipt->>'financials_id','')::uuid
      and timesheet_id=v_target_timesheet_id and is_current=true
    for update;
    if not found or v_daily_fin.processing_status<>'UNPROCESSED' then
      raise exception 'CANDIDATE_DAILY_CANONICAL_RECALCULATION_NOT_READY' using errcode='55000';
    end if;
    v_after_signature:=nullif(v_daily_save_receipt->>'post_save_row_signature','');
    if v_after_signature is null then
      raise exception 'CANDIDATE_DAILY_CANONICAL_SAVE_RECEIPT_INVALID' using errcode='55000';
    end if;
    perform set_config('cloudtms.candidate_electronic_finalise','on',true);
    v_result:=public.timesheet_daily_manual_process_atomic(
      v_target_timesheet_id,v_target_timesheet_id,v_system_actor,
      v_electronic_patch,'{}'::jsonb,p_now_utc,v_after_signature
    );
    v_result:=jsonb_build_object(
      'ok',coalesce((v_result->>'ok')::boolean,false),
      'canonical_save_receipt',v_daily_save_receipt,
      'process_result',v_result,'timesheet_id',v_target_timesheet_id,
      'row_signature',coalesce(v_result->>'row_signature',v_result->>'backend_row_signature')
    );
    end if;
    v_hours_timesheet_id:=v_target_timesheet_id;
    v_after_signature:=coalesce(v_result->>'row_signature',v_result->>'backend_row_signature');
  end if;
  if coalesce((v_result->>'ok')::boolean,false)=false then
    raise exception 'CANDIDATE_FINALISE_CANONICAL_APPLY_FAILED'
      using errcode='55000',detail=jsonb_build_object(
        'code',coalesce(v_result->>'error_code','CANDIDATE_FINALISE_CANONICAL_APPLY_FAILED'),
        'canonical_result',v_result)::text;
  end if;
  if v_target_timesheet_id is null then raise exception 'CANDIDATE_FINALISE_TARGET_MISSING' using errcode='55000'; end if;

  if v_workflow.route<>'PAPER' and v_hours_component.id is not null then
    update public.candidate_submission_components set timesheet_id=coalesce(v_hours_timesheet_id,v_target_timesheet_id)
    where id=v_hours_component.id;
    insert into public.timesheet_evidence(
      timesheet_id,kind,display_name,storage_key,created_at,created_by,
      document_role,candidate_component_id,processing_state
    ) values (
      coalesce(v_hours_timesheet_id,v_target_timesheet_id),'TIMESHEET','Official electronically signed timesheet',
      v_hours_component.final_signed_storage_key,p_now_utc,v_system_actor,
      'SIGNED_TIMESHEET',v_hours_component.id,'READY'
    ) on conflict (candidate_component_id) where candidate_component_id is not null do nothing;
  elsif v_workflow.route='PAPER' and v_workflow.workflow_kind<>'CONTRACT_EXPENSE' then
    select * into v_paper_hours_return
    from public.candidate_submission_components c
    where c.workflow_id=v_workflow.id and c.workflow_generation=v_workflow.generation
      and c.component_kind='SIGNED_RETURN' and c.paper_return_page_key='HOURS_TIMESHEET'
      and c.state='IMMUTABLE' and c.source_content_sha256 is not null
    for update;
    if not found then raise exception 'CANDIDATE_PAPER_RETURN_INCOMPLETE' using errcode='55000'; end if;
    update public.candidate_submission_components set
      timesheet_id=coalesce(v_hours_timesheet_id,v_target_timesheet_id)
    where id=v_paper_hours_return.id;
    insert into public.timesheet_evidence(
      timesheet_id,kind,display_name,storage_key,created_at,created_by,
      document_role,candidate_component_id,processing_state
    ) values (
      coalesce(v_hours_timesheet_id,v_target_timesheet_id),'TIMESHEET','Returned signed paper timesheet',
      v_paper_hours_return.storage_key,p_now_utc,v_system_actor,
      'SIGNED_TIMESHEET',v_paper_hours_return.id,'READY'
    ) on conflict (candidate_component_id) where candidate_component_id is not null do nothing;
  end if;

  v_auto_requested:=coalesce((v_current_policy->>'candidate_electronic_auto_authorise')::boolean,false)
    and v_workflow.route<>'PAPER';
  if v_workflow.issue_codes ?| array[
    'UNEXPECTED_HOURS','DAILY_BREAK_UNEXPECTED','DUPLICATE_EXPENSE_REVIEW',
    'HEALTHROSTER_VALIDATION_REQUIRED','EVIDENCE_REVIEW_REQUIRED',
    'ADDITIONAL_UNITS_NEEDS_CHECKING','PLANNED_HOURS_UNRESOLVED'
  ] then
    v_auto_blocked:=true;
    v_auto_blockers:=v_auto_blockers||v_workflow.issue_codes;
  end if;
  if v_workflow.route='PAPER' then
    v_auto_blocked:=true;v_auto_blockers:=v_auto_blockers||'"PAPER_NEVER_AUTO_AUTHORISES"'::jsonb;
  end if;
  if v_daily_receipt_only then
    v_auto_blocked:=true;
    v_auto_blockers:=v_auto_blockers||'"OFFICE_RESOLUTION_REQUIRED"'::jsonb;
  end if;
  if v_auto_requested and not v_auto_blocked then
    if v_hours_timesheet_id is not null then
      v_authorise_result:=public.timesheet_authorise_generic_atomic(
        v_hours_timesheet_id,v_hours_timesheet_id,v_system_actor,p_now_utc,v_after_signature
      );
      if coalesce((v_authorise_result->>'ok')::boolean,false)=false then
        raise exception 'CANDIDATE_AUTO_AUTHORISE_FAILED' using errcode='55000',detail=v_authorise_result::text;
      end if;
    end if;
    if v_expense_timesheet_id is not null and v_expense_timesheet_id is distinct from v_hours_timesheet_id then
      v_expense_authorise_result:=public.timesheet_authorise_generic_atomic(
        v_expense_timesheet_id,v_expense_timesheet_id,v_system_actor,p_now_utc,null);
      if coalesce((v_expense_authorise_result->>'ok')::boolean,false)=false then
        raise exception 'CANDIDATE_AUTO_AUTHORISE_FAILED' using errcode='55000',detail=v_expense_authorise_result::text;
      end if;
      v_authorise_result:=jsonb_build_object(
        'ok',true,'hours',v_authorise_result,'expenses',v_expense_authorise_result);
    end if;
    if coalesce((v_authorise_result->>'ok')::boolean,false)=false then
      v_auto_blocked:=true;
      v_auto_blockers:=v_auto_blockers||jsonb_build_array(coalesce(v_authorise_result->>'error_code','AUTHORISE_NOT_ADVANCED'));
    end if;
  end if;

  select financials.id into v_canonical_financials_id
  from public.timesheets_financials financials
  where financials.timesheet_id=coalesce(v_hours_timesheet_id,v_target_timesheet_id)
    and financials.is_current=true
  order by financials.computed_at_utc desc nulls last,financials.updated_at desc,financials.id desc
  limit 1;
  if not v_daily_receipt_only then
    if v_canonical_financials_id is null then
      raise exception 'CANDIDATE_CANONICAL_FINANCIALS_NOT_FOUND' using errcode='55000';
    end if;
    v_canonical_financial_sha256:=private._candidate_financial_content_sha256_v1(
      v_canonical_financials_id
    );
  end if;
  -- A received factual claim retains its artifact generation. It is not
  -- financial finalisation and never carries a fabricated financial hash.
  v_completion_generation:=v_workflow.generation+case when v_daily_receipt_only then 0 else 1 end;

  v_response:=jsonb_build_object(
    'ok',true,'idempotent_replay',false,'workflow_id',v_workflow.id,
    'state',v_completion_state,'generation',v_completion_generation,
    'office_resolution_pending',v_daily_receipt_only,
    'timesheet_id',v_target_timesheet_id,'canonical_result',v_result,
    'candidate_auto_authorise_effective',v_auto_requested,
    'auto_authorised',v_auto_requested and not v_auto_blocked,
    'canonical_financial_sha256_hex',encode(v_canonical_financial_sha256,'hex'),
    'auto_authorise_blockers',v_auto_blockers,
    'authorise_result',v_authorise_result
  );
  update public.candidate_submission_workflows set state=v_completion_state,generation=v_completion_generation,
    target_timesheet_id=v_target_timesheet_id,policy_snapshot_json=v_current_policy,
    canonical_financial_sha256=v_canonical_financial_sha256,
    issue_codes=v_workflow.issue_codes,
    finalised_at_utc=case when v_daily_receipt_only then null else p_now_utc end,
    last_mutation_idempotency_key=p_idempotency_key,last_mutation_response_json=v_response,updated_at_utc=p_now_utc
  where id=v_workflow.id;
  perform private._candidate_notification_insert_v1(v_workflow.account_id,v_candidate_id,v_workflow.id,v_target_timesheet_id,
    case when v_auto_requested and not v_auto_blocked then 'AUTHORISED' else 'SUBMISSION_RECEIVED' end,
    'authorisation','candidate-submission-finalised-v1',jsonb_build_object('auto_authorised',v_auto_requested and not v_auto_blocked),
    jsonb_build_object('type','timesheet','timesheet_id',v_target_timesheet_id),
    'CANDIDATE_FINALISED_V1:'||v_workflow.id::text||':'||v_completion_generation::text,p_now_utc);
  perform private._candidate_audit_v1('candidate_submission_workflow',v_workflow.id::text,
    case when v_daily_receipt_only then 'CANDIDATE_SUBMISSION_RECEIVED' else 'CANDIDATE_SUBMISSION_FINALISED' end,
    jsonb_build_object('state',v_workflow.state,'generation',v_workflow.generation),
    jsonb_build_object('state',v_completion_state,'generation',v_completion_generation,'timesheet_id',v_target_timesheet_id,
      'auto_authorised',v_auto_requested and not v_auto_blocked),null,v_system_actor,p_idempotency_key,p_now_utc);
  insert into public.audit_events(
    actor_user_id,object_type,object_id_text,action,before_json,after_json,
    reason,correlation_id,ts_utc
  ) values (
    case when v_is_office_service then nullif(v_service_finalisation->>'actor_user_id','')::uuid
      else null end,
    'candidate_workflow_finalisation_completion',v_workflow.id::text,
    'CANDIDATE_WORKFLOW_FINALISATION_COMPLETED',jsonb_build_object(
      'contract_version','CANDIDATE_FINALISATION_COMPLETION_V1',
      'workflow_generation',p_expected_generation,
      'finalisation_identity_sha256',v_finalisation_identity_hash,
      'finalisation_identity',v_finalisation_identity
    ),v_response,'Canonical finalisation completion receipt',
    p_expected_generation::text||':'||v_finalisation_identity_hash,p_now_utc
  );
  perform private._candidate_workflow_mutation_receipt_v1(
    v_workflow.id,p_idempotency_key,v_mutation_request_hash,'RETRY_FINALISATION',
    case when v_is_office_service then 'OFFICE' when p_session_id is null then 'SERVICE' else 'CANDIDATE_CLIENT' end,
    case when v_is_office_service then v_service_finalisation->>'actor_user_id' else coalesce(p_session_id::text,'SERVICE') end,
    v_response,p_now_utc
  );
  return v_response;
exception
  when unique_violation then
    get stacked diagnostics v_constraint_name=constraint_name;
    if v_constraint_name='timesheet_evidence_one_active_timesheet_uq' then
      raise exception 'TIMESHEET_EVIDENCE_ALREADY_ATTACHED' using errcode='23505';
    end if;
    raise;
end;
$function$;

alter function public.candidate_submission_finalize_atomic_v1(uuid,text,uuid,integer,text,text,timestamptz,jsonb) owner to postgres;
revoke all on function public.candidate_submission_finalize_atomic_v1(uuid,text,uuid,integer,text,text,timestamptz,jsonb) from public,anon,authenticated;
grant execute on function public.candidate_submission_finalize_atomic_v1(uuid,text,uuid,integer,text,text,timestamptz,jsonb) to service_role;

notify pgrst, 'reload schema';


-- Only a terminal, genuinely empty reservation can reach the existing guarded
-- planned-week deletion owner. Active claims, roots and financial rows cannot.
create or replace function private._candidate_empty_provisional_expense_cleanup_v1(
  p_environment text,
  p_contract_week_id uuid,
  p_now_utc timestamptz default now()
)
returns jsonb
language plpgsql
volatile
security definer
set search_path = pg_catalog, public, private, pg_temp
as $function$
declare
  v_environment text:=private._candidate_assert_environment(p_environment);
  v_week public.contract_weeks%rowtype;
  v_actor uuid;
  v_guard jsonb;
begin
  if not private._candidate_provisional_expense_carrier_v1(p_contract_week_id) then
    return jsonb_build_object('deleted',false);
  end if;
  select week_row.* into v_week from public.contract_weeks week_row
  where week_row.id=p_contract_week_id;
  perform 1 from public.candidate_submission_workflows workflow
  where workflow.contract_week_id=p_contract_week_id
  order by workflow.id for update nowait;
  select week_row.* into v_week from public.contract_weeks week_row
  where week_row.id=p_contract_week_id for update nowait;
  if not found or not private._candidate_provisional_expense_carrier_v1(p_contract_week_id) then
    return jsonb_build_object('deleted',false);
  end if;
  -- Keep the worked base present, including at Contract boundary weeks.
  if not exists (
    select 1 from public.contract_weeks base
    where base.contract_id=v_week.contract_id
      and base.week_ending_date=v_week.week_ending_date
      and base.additional_seq=0
  ) then
    return jsonb_build_object('deleted',false);
  end if;
  if not exists (
    select 1 from public.candidate_submission_workflows workflow
    where workflow.contract_week_id=v_week.id and workflow.environment=v_environment
  ) or exists (
    select 1 from public.candidate_submission_workflows workflow
    where workflow.contract_week_id=v_week.id
      and (workflow.environment<>v_environment or workflow.state not in (
        'CANCELLED','EXPIRED','SUPERSEDED','REJECTED','REFUSED'
      ))
  ) or exists (
    select 1 from public.candidate_expense_components component
    join public.candidate_submission_workflows workflow on workflow.id=component.workflow_id
    where workflow.contract_week_id=v_week.id
      and (component.owning_timesheet_id is not null or component.lifecycle_state not in (
        'MANAGER_REFUSED','OFFICE_REJECTED','WITHDRAWN','CANCELLED','SUPERSEDED'
      ))
  ) then
    return jsonb_build_object('deleted',false);
  end if;
  select defaults.candidate_app_system_actor_user_id into v_actor
  from public.settings_defaults defaults where defaults.id=1;
  if v_actor is null then
    raise exception 'CANDIDATE_SYSTEM_ACTOR_NOT_CONFIGURED' using errcode='55000';
  end if;
  v_guard:=private._contract_week_submission_delete_guard_v1(v_environment,v_week.id,false);
  return public.contract_week_delete_planned_guarded_v1(
    v_environment,v_week.id,v_actor,v_guard->>'context_sha256',
    pg_catalog.gen_random_uuid(),p_now_utc
  );
exception when lock_not_available then
  -- Cleanup is opportunistic. A claim creator can already hold the family
  -- week locks while cancellation holds its workflow; never wait in reverse
  -- order and deadlock either live owner. A later ordinary attempt retries.
  return jsonb_build_object('deleted',false,'reason','CLAIM_BUSY');
end;
$function$;
alter function private._candidate_empty_provisional_expense_cleanup_v1(text,uuid,timestamptz)
  owner to postgres;
revoke all on function private._candidate_empty_provisional_expense_cleanup_v1(text,uuid,timestamptz)
  from public,anon,authenticated,service_role;

create or replace function public.candidate_workflow_cancel_atomic_v2(
  p_session_id uuid,
  p_environment text,
  p_workflow_id uuid,
  p_expected_generation integer,
  p_payload jsonb default '{}'::jsonb,
  p_idempotency_key text default null,
  p_now_utc timestamptz default now()
)
returns jsonb
language plpgsql
security definer
set search_path = pg_catalog, public, private, pg_temp
as $function$
declare
  v_result jsonb;
  v_error_message text;
  v_error_detail text;
  v_error_detail_json jsonb;
  v_prepare_result jsonb;
  v_workflow public.candidate_submission_workflows%rowtype;
  v_subject_timesheet_id uuid;
  v_cancel_authority jsonb;
begin
  select workflow_row.* into v_workflow
  from public.candidate_submission_workflows workflow_row
  where workflow_row.id=p_workflow_id
    and workflow_row.environment=private._candidate_assert_environment(p_environment)
  for update;
  -- Cleanup retains the cancellation receipt but detaches the deleted CW.
  -- Let the existing transition owner validate an exact retry before applying
  -- mutable cancellation eligibility to this already-cancelled tombstone.
  if found and v_workflow.workflow_kind='CONTRACT_EXPENSE'
     and v_workflow.state='CANCELLED' and v_workflow.contract_week_id is null
     and v_workflow.input_snapshot_json#>>'{office_permanent_delete_tombstone,previous_contract_week_id}' is not null then
    return public.candidate_workflow_transition_atomic_v1(
      p_session_id,p_environment,p_workflow_id,'CANCEL',p_expected_generation,
      coalesce(p_payload,'{}'::jsonb),p_idempotency_key,p_now_utc);
  end if;
  if found then
    v_subject_timesheet_id:=case
      when v_workflow.workflow_kind='CONTRACT_EXPENSE'
        then v_workflow.target_timesheet_id
      else coalesce(v_workflow.target_timesheet_id,v_workflow.anchor_timesheet_id)
    end;
    if v_subject_timesheet_id is not null then
      perform 1 from public.timesheets subject_timesheet
      where subject_timesheet.timesheet_id=v_subject_timesheet_id
      for update;
      perform 1 from public.timesheets_financials financial
      where financial.timesheet_id=v_subject_timesheet_id
        and financial.is_current=true
      for update;
    end if;
    v_cancel_authority:=private._candidate_workflow_cancel_authority_v1(v_workflow.id);
    if not coalesce((v_cancel_authority->>'eligible')::boolean,false) then
      raise exception 'CANDIDATE_WORKFLOW_NOT_CANCELLABLE'
        using errcode='55000',detail=v_cancel_authority::text;
    end if;
  end if;

  begin
    v_result:=public.candidate_workflow_transition_atomic_v1(
      p_session_id,p_environment,p_workflow_id,'CANCEL',
      p_expected_generation,coalesce(p_payload,'{}'::jsonb),
      p_idempotency_key,p_now_utc
    );
    perform private._candidate_empty_provisional_expense_cleanup_v1(
      p_environment,v_workflow.contract_week_id,p_now_utc
    );
    return v_result;
  exception when sqlstate '40001' then
    get stacked diagnostics
      v_error_message=message_text,
      v_error_detail=pg_exception_detail;
    begin
      v_error_detail_json:=coalesce(nullif(v_error_detail,''),'{}')::jsonb;
    exception when others then
      raise;
    end;
    if v_error_message<>'CANDIDATE_PAPER_QR_SOURCE_CONFLICT'
       or v_error_detail_json->>'code'<>'CANDIDATE_PAPER_QR_SOURCE_CONFLICT'
       or v_error_detail_json->>'reason'<>'CURRENT_QR_TOKEN_OWNER_CONFLICT'
       or coalesce((v_error_detail_json->>'owner_count')::integer,-1)<>0 then
      raise;
    end if;
  end;

  begin
    v_prepare_result:=private._candidate_legacy_paper_orphan_prepare_v1(
      p_environment,p_workflow_id,p_expected_generation,p_now_utc
    );
  exception when sqlstate '40001' then
    -- A current manifest that is not the exact retired one-page shape keeps the
    -- ordinary owner's existing conflict.  The compatibility adapter must not
    -- change the error contract for modern page-manifest packs.
    if sqlerrm='CANDIDATE_LEGACY_PAPER_ORPHAN_NOT_ELIGIBLE' then
      raise exception '%',v_error_message
        using errcode='40001',detail=v_error_detail;
    end if;
    raise;
  end;
  if not coalesce((v_prepare_result->>'prepared')::boolean,false) then
    raise exception 'CANDIDATE_LEGACY_PAPER_ORPHAN_PREPARATION_FAILED'
      using errcode='40001';
  end if;

  -- Re-enter the unchanged cancellation authority.  If it fails, the token
  -- retirement above rolls back with it; there is no partial compatibility
  -- state and no alternate withdrawal implementation.
  v_result:=public.candidate_workflow_transition_atomic_v1(
    p_session_id,p_environment,p_workflow_id,'CANCEL',
    p_expected_generation,coalesce(p_payload,'{}'::jsonb),
    p_idempotency_key,p_now_utc
  );
  perform private._candidate_empty_provisional_expense_cleanup_v1(
    p_environment,v_workflow.contract_week_id,p_now_utc
  );
  return v_result;
end;
$function$;

alter function public.candidate_workflow_cancel_atomic_v2(uuid,text,uuid,integer,jsonb,text,timestamptz) owner to postgres;
revoke all on function public.candidate_workflow_cancel_atomic_v2(uuid,text,uuid,integer,jsonb,text,timestamptz) from public,anon,authenticated;
grant execute on function public.candidate_workflow_cancel_atomic_v2(uuid,text,uuid,integer,jsonb,text,timestamptz) to service_role;


CREATE OR REPLACE FUNCTION public.contract_week_delete_planned(
  p_contract_week_id uuid,
  p_actor_user_id uuid
)
RETURNS TABLE (
  deleted boolean,
  contract_week_id uuid
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path TO 'public', 'pg_temp'
AS $function$
DECLARE
  v_deleted boolean;
  v_preserve_contract_boundary boolean;
  v_deleted_contract_week_id uuid;
  v_contract_id uuid;
  v_contract_start date;
  v_contract_end date;
  v_week_ending_weekday integer;
  v_first_remaining_week date;
  v_last_remaining_week date;
  v_start_week date;
  v_end_week date;
  v_first_remaining_planned_date date;
  v_last_remaining_planned_date date;
  v_new_start date;
  v_new_end date;
BEGIN
  v_preserve_contract_boundary:=private._candidate_provisional_expense_carrier_v1(p_contract_week_id);
  SELECT cw.contract_id
  INTO v_contract_id
  FROM public.contract_weeks cw
  WHERE cw.id = p_contract_week_id;

  SELECT result.deleted, result.contract_week_id
  INTO v_deleted, v_deleted_contract_week_id
  FROM private.contract_week_delete_planned_base_v1(
    p_contract_week_id,
    p_actor_user_id
  ) AS result;

  IF v_deleted IS NOT TRUE OR v_contract_id IS NULL OR v_preserve_contract_boundary THEN
    deleted := v_deleted;
    contract_week_id := v_deleted_contract_week_id;
    RETURN NEXT;
    RETURN;
  END IF;

  SELECT c.start_date, c.end_date, COALESCE(c.week_ending_weekday_snapshot, 0)
  INTO v_contract_start, v_contract_end, v_week_ending_weekday
  FROM public.contracts c
  WHERE c.id = v_contract_id
  FOR UPDATE;

  IF FOUND THEN
    SELECT min(cw.week_ending_date), max(cw.week_ending_date)
    INTO v_first_remaining_week, v_last_remaining_week
    FROM public.contract_weeks cw
    WHERE cw.contract_id = v_contract_id;

    v_new_start := v_contract_start;
    v_new_end := v_contract_end;

    IF v_first_remaining_week IS NOT NULL THEN
      v_start_week := v_contract_start
        + mod(v_week_ending_weekday - extract(dow from v_contract_start)::integer + 7, 7);
      v_end_week := v_contract_end
        + mod(v_week_ending_weekday - extract(dow from v_contract_end)::integer + 7, 7);

      IF v_first_remaining_week > v_start_week THEN
        SELECT min((entry.item ->> 'date')::date)
        INTO v_first_remaining_planned_date
        FROM public.contract_weeks remaining
        CROSS JOIN LATERAL jsonb_array_elements(COALESCE(remaining.planned_schedule_json, '[]'::jsonb)) entry(item)
        WHERE remaining.contract_id = v_contract_id
          AND remaining.week_ending_date = v_first_remaining_week
          AND jsonb_typeof(entry.item) = 'object'
          AND COALESCE(entry.item ->> 'date', '') ~ '^\d{4}-\d{2}-\d{2}$';

        v_new_start := COALESCE(v_first_remaining_planned_date, v_first_remaining_week - 6);
      END IF;

      IF v_last_remaining_week < v_end_week THEN
        SELECT max((entry.item ->> 'date')::date)
        INTO v_last_remaining_planned_date
        FROM public.contract_weeks remaining
        CROSS JOIN LATERAL jsonb_array_elements(COALESCE(remaining.planned_schedule_json, '[]'::jsonb)) entry(item)
        WHERE remaining.contract_id = v_contract_id
          AND remaining.week_ending_date = v_last_remaining_week
          AND jsonb_typeof(entry.item) = 'object'
          AND COALESCE(entry.item ->> 'date', '') ~ '^\d{4}-\d{2}-\d{2}$';

        v_new_end := COALESCE(v_last_remaining_planned_date, v_last_remaining_week);
      END IF;

      IF v_new_start IS DISTINCT FROM v_contract_start
         OR v_new_end IS DISTINCT FROM v_contract_end THEN
        UPDATE public.contracts
        SET start_date = v_new_start,
            end_date = v_new_end
        WHERE id = v_contract_id;

        INSERT INTO public.audit_events(
          actor_user_id,
          object_type,
          object_id_text,
          action,
          before_json,
          after_json,
          reason
        )
        VALUES (
          p_actor_user_id,
          'contract',
          v_contract_id::text,
          'CONTRACT_DATES_RECONCILED_AFTER_WEEK_DELETE',
          jsonb_build_object('start_date', v_contract_start, 'end_date', v_contract_end),
          jsonb_build_object('start_date', v_new_start, 'end_date', v_new_end),
          'PLANNED_CONTRACT_WEEK_DELETED'
        );
      END IF;
    END IF;
  END IF;

  deleted := v_deleted;
  contract_week_id := v_deleted_contract_week_id;
  RETURN NEXT;
END;
$function$;

REVOKE ALL ON FUNCTION public.contract_week_delete_planned(uuid, uuid) FROM PUBLIC;

DO $do$
BEGIN
  IF EXISTS (SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = 'anon') THEN
    EXECUTE 'REVOKE ALL ON FUNCTION public.contract_week_delete_planned(uuid, uuid) FROM anon';
  END IF;
  IF EXISTS (SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = 'authenticated') THEN
    EXECUTE 'REVOKE ALL ON FUNCTION public.contract_week_delete_planned(uuid, uuid) FROM authenticated';
  END IF;
  IF EXISTS (SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = 'service_role') THEN
    EXECUTE 'GRANT EXECUTE ON FUNCTION public.contract_week_delete_planned(uuid, uuid) TO service_role';
  END IF;
END
$do$;


create or replace function public.candidate_expense_component_action_atomic_v1(
  p_session_id uuid,
  p_environment text,
  p_workflow_id uuid,
  p_expected_generation integer,
  p_expense_component_id uuid,
  p_expected_component_generation integer,
  p_action text,
  p_idempotency_key text,
  p_now_utc timestamptz default now()
)
returns jsonb
language plpgsql
volatile
security definer
set search_path = pg_catalog, public, private, pg_temp
as $function$
declare
  v_environment text:=private._candidate_assert_environment(p_environment);
  v_action text:=upper(btrim(coalesce(p_action,'')));
  v_context jsonb;
  v_workflow public.candidate_submission_workflows%rowtype;
  v_component public.candidate_expense_components%rowtype;
  v_before jsonb;
  v_financial jsonb;
  v_begin jsonb;
  v_zero boolean:=false;
  v_delete_result jsonb:='{}'::jsonb;
  v_request jsonb;
  v_request_sha bytea;
  v_operation public.candidate_expense_operations%rowtype;
  v_pending_update public.candidate_pending_expense_updates%rowtype;
  v_result jsonb;
  v_removal_context jsonb;
  v_direct_empty_pending boolean:=false;
begin
  if p_workflow_id is null or p_expected_generation is null
     or p_expense_component_id is null or p_expected_component_generation is null
     or v_action not in ('REMOVE_EXPENSE','WITHDRAW_EXPENSE','CANCEL_EXPENSE')
     or nullif(btrim(coalesce(p_idempotency_key,'')),'') is null then
    raise exception 'CANDIDATE_EXPENSE_COMPONENT_ACTION_INVALID' using errcode='22023';
  end if;
  v_context:=private._candidate_session_context_v1(
    p_session_id,v_environment,null,p_now_utc,true
  );
  -- Resolve an exact replay before consulting mutable workflow/component
  -- generations.  A successful category action necessarily advances those
  -- generations, so checking them first would make a lost 200 response
  -- impossible to reconcile safely.
  v_request:=jsonb_build_object(
    'contract_version','CANDIDATE_EXPENSE_CATEGORY_ACTION_REQUEST_V1',
    'workflow_id',p_workflow_id,'workflow_generation',p_expected_generation,
    'expense_component_id',p_expense_component_id,
    'component_generation',p_expected_component_generation,'action_code',v_action
  );
  v_request_sha:=private._candidate_sha256_jsonb_v1(v_request);
  perform pg_advisory_xact_lock(hashtextextended(
    'candidate-expense-operation|'||v_environment||'|'
      ||(v_context->>'selected_candidate_id')||'|'||btrim(p_idempotency_key),0
  ));
  select operation.* into v_operation
  from public.candidate_expense_operations operation
  where operation.environment=v_environment
    and operation.actor_kind='CANDIDATE'
    and operation.actor_id=(v_context->>'selected_candidate_id')::uuid
    and operation.idempotency_key=btrim(p_idempotency_key)
  for update;
  if found then
    if v_operation.action_code<>v_action or v_operation.request_sha256<>v_request_sha then
      raise exception 'CANDIDATE_IDEMPOTENCY_CONFLICT' using errcode='23505';
    end if;
    if v_operation.state='COMMITTED' then
      return v_operation.result_json||jsonb_build_object('idempotent_replay',true);
    end if;
    if v_operation.state in ('FAILED','ABORTED')
       and v_operation.result_json is not null then
      return v_operation.result_json||jsonb_build_object('idempotent_replay',true);
    end if;
    if v_operation.state='RENDERING' and v_operation.progress_json is not null then
      select update_row.* into v_pending_update
      from public.candidate_pending_expense_updates update_row
      where update_row.operation_id=v_operation.operation_id
        and update_row.workflow_id=v_operation.workflow_id
        and update_row.actor_kind='CANDIDATE'
        and update_row.actor_id=v_operation.candidate_id
        and update_row.state in ('EDITING','RENDERING')
      for update;
      if found and v_pending_update.state='RENDERING'
         and jsonb_typeof(v_pending_update.submit_result_json)='object'
         and v_pending_update.submit_result_json->>'update_id'=v_pending_update.update_id::text
         and v_pending_update.submit_result_json->>'workflow_id'=v_operation.workflow_id::text
         and (v_pending_update.submit_result_json->>'generation')::integer
           =v_pending_update.current_workflow_generation
         and v_pending_update.submit_result_json->>'update_state'='UPDATING'
         and jsonb_typeof(v_pending_update.submit_result_json->'render_contract')='object' then
        return v_operation.progress_json||v_pending_update.submit_result_json
          ||jsonb_build_object(
            'ok',true,
            'contract_version','CANDIDATE_EXPENSE_CATEGORY_ACTION_RESULT_V1',
            'expense_component_id',v_operation.expense_component_id,
            'idempotent_replay',true
          );
      end if;
      return v_operation.progress_json||jsonb_build_object(
        'ok',true,
        'contract_version','CANDIDATE_EXPENSE_CATEGORY_ACTION_RESULT_V1',
        'expense_component_id',v_operation.expense_component_id,
        'idempotent_replay',true
      );
    end if;
    raise exception 'CANDIDATE_EXPENSE_OPERATION_IN_PROGRESS' using errcode='55000';
  end if;
  select workflow.* into v_workflow from public.candidate_submission_workflows workflow
  where workflow.id=p_workflow_id and workflow.environment=v_environment
    and workflow.account_id=(v_context->>'account_id')::uuid
    and workflow.candidate_id=(v_context->>'selected_candidate_id')::uuid
  for update;
  if not found then raise exception 'CANDIDATE_WORKFLOW_NOT_FOUND' using errcode='P0002'; end if;
  select component.* into v_component from public.candidate_expense_components component
  where component.expense_component_id=p_expense_component_id
    and component.workflow_id=v_workflow.id
  for update;
  if not found then raise exception 'CANDIDATE_EXPENSE_COMPONENT_NOT_FOUND' using errcode='P0002'; end if;
  if v_workflow.generation<>p_expected_generation
     or v_component.component_generation<>p_expected_component_generation then
    raise exception 'CANDIDATE_EXPENSE_COMPONENT_CHANGED' using errcode='40001';
  end if;
  if exists(
    select 1 from public.timesheets owner
    where owner.timesheet_id in (
      v_component.owning_timesheet_id,
      v_workflow.target_timesheet_id,
      v_workflow.anchor_timesheet_id
    ) and owner.sheet_scope='DAILY'::public.timesheet_scope_enum
  ) then
    raise exception 'CANDIDATE_EXPENSE_COMPONENT_CHANGED' using errcode='40001';
  end if;
  if v_component.agency_authorisation_state not in ('NOT_AUTHORISED','PAID') then
    raise exception 'CANDIDATE_EXPENSE_COMPONENT_PROTECTED' using errcode='55000';
  end if;

  -- A client can lose the first HTTP response after the exact pending-manager
  -- update and its operation have already been durably created. A refreshed
  -- client then has a new idempotency key and sees the advanced WORKER_DRAFT
  -- generation. Resume only the one locked active update whose immutable
  -- removal plan, candidate operation and progress receipt all identify this
  -- exact component action. Never broaden recovery to another category,
  -- component, candidate or operation.
  if v_action='WITHDRAW_EXPENSE' then
    select update_row.* into v_pending_update
    from public.candidate_pending_expense_updates update_row
    where update_row.workflow_id=v_workflow.id
      and update_row.state in ('EDITING','RENDERING')
      and update_row.update_mode='PENDING_MANAGER'
      and update_row.actor_kind='CANDIDATE'
      and update_row.actor_id=v_workflow.candidate_id
      and update_row.current_workflow_generation=v_workflow.generation
      and p_expected_generation in (
        update_row.from_workflow_generation,
        update_row.current_workflow_generation
      )
      and jsonb_strip_nulls(update_row.update_plan_json)=jsonb_build_array(
        jsonb_build_object(
          'update_kind','REMOVE_CATEGORY',
          'expense_category',v_component.expense_category,
          'expense_component_id',v_component.expense_component_id,
          'component_generation',v_component.component_generation
        )
      )
      and update_row.operation_id is not null
    for update;
    if found then
      select operation.* into v_operation
      from public.candidate_expense_operations operation
      where operation.operation_id=v_pending_update.operation_id
        and operation.environment=v_environment
        and operation.account_id=v_workflow.account_id
        and operation.candidate_id=v_workflow.candidate_id
        and operation.actor_kind='CANDIDATE'
        and operation.actor_id=v_workflow.candidate_id
        and operation.action_code=v_action
        and operation.workflow_id=v_workflow.id
        and operation.expense_component_id=v_component.expense_component_id
        and operation.state='RENDERING'
      for update;
      if found
         and jsonb_typeof(v_operation.progress_json)='object'
         and v_operation.progress_json->>'operation_id'=v_operation.operation_id::text
         and v_operation.progress_json->>'update_id'=v_pending_update.update_id::text
         and v_operation.progress_json->>'workflow_id'=v_workflow.id::text
         and v_operation.progress_json->>'action_code'=v_action
         and coalesce(
           (v_operation.progress_json->>'automatic_resubmission_required')::boolean,
           false
         ) then
        if v_pending_update.state='EDITING'
           and (v_operation.progress_json->>'generation')::integer
             =v_pending_update.current_workflow_generation then
          return v_operation.progress_json||jsonb_build_object(
            'ok',true,
            'contract_version','CANDIDATE_EXPENSE_CATEGORY_ACTION_RESULT_V1',
            'expense_component_id',v_component.expense_component_id,
            'idempotent_replay',true
          );
        end if;
        if v_pending_update.state='RENDERING'
           and jsonb_typeof(v_pending_update.submit_result_json)='object'
           and v_pending_update.submit_result_json->>'update_id'=v_pending_update.update_id::text
           and v_pending_update.submit_result_json->>'workflow_id'=v_workflow.id::text
           and (v_pending_update.submit_result_json->>'generation')::integer
             =v_pending_update.current_workflow_generation
           and v_pending_update.submit_result_json->>'update_state'='UPDATING'
           and jsonb_typeof(v_pending_update.submit_result_json->'render_contract')='object' then
          return v_operation.progress_json||v_pending_update.submit_result_json
            ||jsonb_build_object(
              'ok',true,
              'contract_version','CANDIDATE_EXPENSE_CATEGORY_ACTION_RESULT_V1',
              'expense_component_id',v_component.expense_component_id,
              'idempotent_replay',true
            );
        end if;
        raise exception 'CANDIDATE_EXPENSE_OPERATION_IN_PROGRESS' using errcode='55000';
      end if;
      raise exception 'CANDIDATE_EXPENSE_OPERATION_IN_PROGRESS' using errcode='55000';
    end if;
  end if;

  if v_action='WITHDRAW_EXPENSE' and v_workflow.route='PAPER'
     and v_workflow.state='AWAITING_PAPER_RETURN'
     and v_component.lifecycle_state='SUBMITTED'
     and v_component.manager_approval_state='NOT_REQUESTED' then
    return jsonb_build_object(
      'ok',true,'contract_version','CANDIDATE_EXPENSE_CATEGORY_ACTION_RESULT_V1',
      'operation_id',null,'workflow_id',v_workflow.id,'generation',v_workflow.generation,
      'expense_component_id',v_component.expense_component_id,
      'component_generation',v_component.component_generation,
      'state',v_component.lifecycle_state,'update_state','NONE',
      'action_code',v_action,'paper_replacement_required',true,
      'empty_timesheet_consequence','NONE',
      'removed_from_current_timesheet_ids','[]'::jsonb,
      'paper_replacement_action','CREATE_UPDATED_DOCUMENTS',
      'paper_replacement_category_changes',jsonb_build_array(jsonb_build_object(
        'update_kind','REMOVE_CATEGORY',
        'expense_category',v_component.expense_category,
        'expense_component_id',v_component.expense_component_id,
        'component_generation',v_component.component_generation
      )),
      'idempotent_replay',false
    );
  end if;

  if (v_action='REMOVE_EXPENSE' and v_component.lifecycle_state<>'DRAFT')
     or (v_action='WITHDRAW_EXPENSE' and not (
       v_component.lifecycle_state='SUBMITTED'
       and (
         (v_component.manager_approval_state='PENDING' and v_workflow.route<>'PAPER')
         or (v_component.manager_approval_state='NOT_REQUESTED'
           and v_workflow.route='PAPER'
           and v_workflow.state='AWAITING_PAPER_RETURN')
       )
     ))
     or (v_action='CANCEL_EXPENSE' and not (
       v_component.lifecycle_state='MANAGER_APPROVED'
       and v_component.manager_approval_state='APPROVED'
       and v_workflow.state='FINALISED'
     )) then
    raise exception 'CANDIDATE_EXPENSE_COMPONENT_ACTION_NOT_ALLOWED' using errcode='55000';
  end if;
  insert into public.candidate_expense_operations(
    environment,account_id,candidate_id,actor_kind,actor_id,action_code,
    workflow_id,timesheet_id,expense_component_id,request_sha256,idempotency_key,
    state,created_at_utc,updated_at_utc
  ) values (
    v_environment,v_workflow.account_id,v_workflow.candidate_id,'CANDIDATE',
    v_workflow.candidate_id,v_action,v_workflow.id,
    private._candidate_expense_owned_timesheet_id_v1(
      v_workflow.id,v_component.owning_timesheet_id
    ),v_component.expense_component_id,v_request_sha,btrim(p_idempotency_key),
    'PREPARING',p_now_utc,p_now_utc
  ) returning * into v_operation;

  if v_action='WITHDRAW_EXPENSE' and v_component.lifecycle_state='SUBMITTED'
     and v_component.manager_approval_state='PENDING'
     and v_workflow.route<>'PAPER' then
    v_removal_context:=private._candidate_office_expense_rejection_context_v1(
      v_environment,v_component.expense_component_id,p_now_utc
    );
    v_direct_empty_pending:=v_workflow.workflow_kind='CONTRACT_EXPENSE'
      and (
        coalesce(v_removal_context#>>'{basis,empty_timesheet_consequence}','NONE')
          <>'NONE'
        or (
          v_workflow.target_timesheet_id is null
          and v_component.owning_timesheet_id is null
        )
      )
      and not exists(
        select 1 from public.candidate_expense_components other_component
        where other_component.workflow_id=v_workflow.id
          and other_component.expense_component_id<>v_component.expense_component_id
          and other_component.lifecycle_state not in (
            'MANAGER_REFUSED','OFFICE_REJECTED','WITHDRAWN','CANCELLED','SUPERSEDED'
          )
    );
    if not v_direct_empty_pending then
      v_begin:=public.candidate_expense_update_begin_atomic_v1(
        p_session_id,v_environment,v_workflow.id,v_workflow.generation,
        jsonb_build_array(jsonb_build_object(
          'update_kind','REMOVE_CATEGORY',
          'expense_category',v_component.expense_category,
          'expense_component_id',v_component.expense_component_id,
          'component_generation',v_component.component_generation
        )),'category-withdraw-begin:'||btrim(p_idempotency_key),p_now_utc
      );
      update public.candidate_pending_expense_updates set
        operation_id=v_operation.operation_id,updated_at_utc=p_now_utc
      where update_id=(v_begin->>'update_id')::uuid and state='EDITING';
      if not found then
        raise exception 'CANDIDATE_EXPENSE_UPDATE_APPROVAL_CHANGED' using errcode='40001';
      end if;
      update public.candidate_expense_operations set state='RENDERING',
        progress_json=v_begin||jsonb_build_object(
          'ok',true,
          'contract_version','CANDIDATE_EXPENSE_CATEGORY_ACTION_RESULT_V1',
          'operation_id',v_operation.operation_id,'action_code',v_action,
          'expense_component_id',v_component.expense_component_id,
          'automatic_resubmission_required',true
        ),updated_at_utc=p_now_utc
      where operation_id=v_operation.operation_id and state='PREPARING';
      return v_begin||jsonb_build_object(
        'ok',true,
        'contract_version','CANDIDATE_EXPENSE_CATEGORY_ACTION_RESULT_V1',
        'operation_id',v_operation.operation_id,'action_code',v_action,
        'expense_component_id',v_component.expense_component_id,
        'automatic_resubmission_required',true,'idempotent_replay',false
      );
    end if;
    perform private._candidate_empty_manager_request_cancel_v1(
      v_workflow.id,v_workflow.generation,'EXPENSE_CATEGORY_WITHDRAWN',p_now_utc
    );
  end if;
  v_before:=to_jsonb(v_component);
  -- Always return the same closed financial receipt.  A target-less draft is a
  -- proved no-op (`financial_changed=false`), not an untyped empty object.
  v_financial:=private._candidate_expense_financial_remove_v1(
    v_component.expense_component_id,v_component.component_generation,p_now_utc
  );
  v_zero:=coalesce((v_financial->>'zero_expense_carrier')::boolean,false);
  if v_action='REMOVE_EXPENSE' then
    update public.candidate_submission_workflows set
      input_snapshot_json=private._candidate_expense_submission_without_category_v1(
        input_snapshot_json,v_component.expense_category
      ),
      immutable_submission_json=case when immutable_submission_json is null then null
        else private._candidate_expense_submission_without_category_v1(
          immutable_submission_json,v_component.expense_category
        ) end,
      immutable_submission_sha256=case when immutable_submission_json is null then null
        else private._candidate_sha256_jsonb_v1(
          private._candidate_expense_submission_without_category_v1(
            immutable_submission_json,v_component.expense_category
          )
        ) end,
      updated_at_utc=p_now_utc
    where id=v_workflow.id and generation=v_workflow.generation;
  end if;
  update public.candidate_submission_components set
    state='SUPERSEDED',superseded_at_utc=p_now_utc,
    review_render_state=case when review_render_state='NOT_REQUIRED'
      then review_render_state else 'SUPERSEDED' end,
    final_signed_render_state=case when final_signed_render_state='NOT_REQUIRED'
      then final_signed_render_state else 'SUPERSEDED' end
  where workflow_id=v_workflow.id
    and expense_category=v_component.expense_category
    and state<>'SUPERSEDED';
  update public.candidate_expense_components set
    component_generation=component_generation+1,
    lifecycle_state=case when v_action='REMOVE_EXPENSE' then 'SUPERSEDED'
      when v_action='WITHDRAW_EXPENSE' then 'WITHDRAWN' else 'CANCELLED' end,
    manager_approval_state=case when v_action='CANCEL_EXPENSE'
      then manager_approval_state else 'NOT_REQUESTED' end,
    approval_request_id=case when v_action='CANCEL_EXPENSE'
      then approval_request_id else null end,
    removed_at_utc=p_now_utc,updated_at_utc=p_now_utc
  where expense_component_id=v_component.expense_component_id
  returning * into v_component;
  insert into public.candidate_expense_component_events(
    expense_component_id,workflow_id,component_generation,event_type,actor_kind,
    actor_id,before_state_json,after_state_json,idempotency_key,occurred_at_utc
  ) values (
    v_component.expense_component_id,v_workflow.id,v_component.component_generation,
    case when v_action='REMOVE_EXPENSE' then 'SUPERSEDED'
      when v_action='WITHDRAW_EXPENSE' then 'WITHDRAWN' else 'CANCELLED' end,
    'CANDIDATE',v_workflow.candidate_id,v_before,to_jsonb(v_component),
    'candidate-category-action:'||btrim(p_idempotency_key),p_now_utc
  );
  -- Removing an unsubmitted draft is an immediate Candidate edit, not a
  -- cancellation event. Durable/in-app and push-outbox notices are reserved
  -- for a submitted withdrawal or an approved-category cancellation.
  if v_action in ('WITHDRAW_EXPENSE','CANCEL_EXPENSE') then
    perform private._candidate_notification_insert_v1(
      v_workflow.account_id,v_workflow.candidate_id,v_workflow.id,
      nullif(v_financial->>'timesheet_id','')::uuid,
      case when v_action='WITHDRAW_EXPENSE' then 'EXPENSE_WITHDRAWN'
        else 'EXPENSE_CANCELLED' end,'timesheet_expense_attention',
      case when v_action='WITHDRAW_EXPENSE'
        then 'candidate-expense-category-withdrawn-v1'
        else 'candidate-expense-category-cancelled-v1' end,jsonb_build_object(
        'workflow_id',v_workflow.id,
        'expense_component_id',v_component.expense_component_id,
        'expense_category',v_component.expense_category
      ),jsonb_build_object('type','workflow','workflow_id',v_workflow.id),
      case when v_action='WITHDRAW_EXPENSE'
        then 'CANDIDATE_EXPENSE_CATEGORY_WITHDRAWN_V1:'
        else 'CANDIDATE_EXPENSE_CATEGORY_CANCELLED_V1:' end
        ||v_component.expense_component_id::text
        ||':'||v_component.component_generation::text,p_now_utc
    );
  end if;
  if v_direct_empty_pending then
    update public.candidate_submission_workflows workflow set
      state='CANCELLED',generation=workflow.generation+1,
      cancelled_at_utc=p_now_utc,updated_at_utc=p_now_utc
    where workflow.id=v_workflow.id
      and workflow.generation=v_workflow.generation
      and workflow.state='AWAITING_MANAGER_APPROVAL'
    returning workflow.* into v_workflow;
    if not found then
      raise exception 'CANDIDATE_EXPENSE_UPDATE_APPROVAL_CHANGED' using errcode='40001';
    end if;
  end if;
  if v_zero and nullif(v_financial->>'timesheet_id','') is not null then
    v_delete_result:=private._candidate_zero_expense_carrier_delete_v1(
      v_environment,(v_financial->>'timesheet_id')::uuid,
      v_operation.operation_id,p_now_utc
    );
  elsif nullif(v_financial->>'timesheet_id','') is not null then
    v_delete_result:=jsonb_build_object(
      'owning_timesheet_deleted',false,
      'empty_timesheet_consequence','NONE',
      'deleted_timesheet_ids','[]'::jsonb,
      'retained_timesheet_ids',jsonb_build_array(v_financial->'timesheet_id'),
      'affected_timesheet_ids',jsonb_build_array(v_financial->'timesheet_id'),
      'removed_from_current_timesheet_ids','[]'::jsonb,
      'r2_cleanup_keys','[]'::jsonb
    );
  end if;
  v_result:=jsonb_build_object(
    'ok',true,'contract_version','CANDIDATE_EXPENSE_CATEGORY_ACTION_RESULT_V1',
    'operation_id',v_operation.operation_id,
    'workflow_id',v_workflow.id,'generation',v_workflow.generation,
    'expense_component_id',v_component.expense_component_id,
    'component_generation',v_component.component_generation,
    'state',v_component.lifecycle_state,'update_state','NONE',
    'action_code',v_action,'financial_result',coalesce(v_financial,'{}'::jsonb),
    'zero_expense_carrier',v_zero,
    'empty_timesheet_consequence',coalesce(
      v_delete_result->>'empty_timesheet_consequence','NONE'
    ),
    'owning_timesheet_deleted',coalesce(
      (v_delete_result->>'owning_timesheet_deleted')::boolean,false
    ),
    'deleted_timesheet_ids',coalesce(v_delete_result->'deleted_timesheet_ids','[]'::jsonb),
    'retained_timesheet_ids',coalesce(v_delete_result->'retained_timesheet_ids','[]'::jsonb),
    'affected_timesheet_ids',coalesce(v_delete_result->'affected_timesheet_ids','[]'::jsonb),
    'removed_from_current_timesheet_ids',coalesce(
      v_delete_result->'removed_from_current_timesheet_ids','[]'::jsonb
    ),
    'r2_cleanup_keys',coalesce(v_delete_result->'r2_cleanup_keys','[]'::jsonb),
    'idempotent_replay',false
  );
  update public.candidate_expense_operations operation set
    state='COMMITTED',result_json=v_result,completed_at_utc=p_now_utc,
    updated_at_utc=p_now_utc
  where operation.operation_id=v_operation.operation_id and operation.state='PREPARING';
  if not found then
    raise exception 'CANDIDATE_EXPENSE_OPERATION_CHANGED' using errcode='40001';
  end if;
  perform private._candidate_empty_provisional_expense_cleanup_v1(
    v_environment,v_workflow.contract_week_id,p_now_utc
  );
  return v_result;
end;
$function$;
alter function public.candidate_expense_component_action_atomic_v1(uuid,text,uuid,integer,uuid,integer,text,text,timestamptz) owner to postgres;
revoke all on function public.candidate_expense_component_action_atomic_v1(uuid,text,uuid,integer,uuid,integer,text,text,timestamptz) from public,anon,authenticated,service_role;
grant execute on function public.candidate_expense_component_action_atomic_v1(uuid,text,uuid,integer,uuid,integer,text,text,timestamptz) to service_role;


create or replace function public.expense_carrier_resolve_or_create_atomic_v1(
  p_candidate_id uuid,
  p_environment text,
  p_anchor_timesheet_id uuid,
  p_expected_row_signature text,
  p_idempotency_key text,
  p_now_utc timestamptz default now()
)
returns jsonb
language plpgsql
security definer
set search_path = pg_catalog, public, private, pg_temp
as $function$
declare
  v_environment text;
  v_cleanup_week_id uuid;
  v_anchor_week public.contract_weeks%rowtype;
  v_contract public.contracts%rowtype;
  v_placement jsonb;
  v_signature jsonb;
  v_new_week public.contract_weeks%rowtype;
  v_next_seq integer;
  v_bound_count integer:=0;
  v_bound_week public.contract_weeks%rowtype;
begin
  v_environment:=private._candidate_assert_environment(p_environment);
  perform private._candidate_require_feature_v1(v_environment,'candidate_expense_atomic_placement');
  if p_candidate_id is null or p_anchor_timesheet_id is null or nullif(btrim(coalesce(p_idempotency_key,'')),'') is null then
    raise exception 'EXPENSE_CARRIER_PAYLOAD_INVALID' using errcode='22023';
  end if;
  select * into v_anchor_week from public.contract_weeks where timesheet_id=p_anchor_timesheet_id for update;
  if not found then raise exception 'EXPENSE_PLACEMENT_ANCHOR_NOT_FOUND' using errcode='P0002'; end if;
  select * into v_contract from public.contracts where id=v_anchor_week.contract_id and candidate_id=p_candidate_id for update;
  if not found then raise exception 'EXPENSE_PLACEMENT_CANDIDATE_MISMATCH' using errcode='28000'; end if;
  if nullif(btrim(coalesce(p_expected_row_signature,'')),'') is not null then
    v_signature:=public.timesheet_lifecycle_guard_signature_v1(p_anchor_timesheet_id,v_anchor_week.id,false);
    if coalesce(v_signature->>'row_signature',v_signature->>'backend_row_signature','')<>p_expected_row_signature then
      raise exception 'ROW_SIGNATURE_MISMATCH'
        using errcode='40001',detail=jsonb_build_object('code','ROW_SIGNATURE_MISMATCH')::text;
    end if;
  end if;
  perform pg_advisory_xact_lock(hashtext(v_contract.id::text||'|'||v_anchor_week.week_ending_date::text||'|EXPENSE_CARRIER'));
  perform 1 from public.contract_weeks cw
  where cw.contract_id=v_contract.id and cw.week_ending_date=v_anchor_week.week_ending_date
  order by cw.additional_seq,cw.id for update;

  -- Retire only audited, empty, terminal reservations in this exact family.
  -- Active/pending approval and READY_TO_FINALISE carriers are never deleted.
  for v_cleanup_week_id in
    select cw.id from public.contract_weeks cw
    where cw.contract_id=v_contract.id
      and cw.week_ending_date=v_anchor_week.week_ending_date
      and cw.additional_seq>0 and cw.timesheet_id is null
    order by cw.additional_seq,cw.id
  loop
    perform private._candidate_empty_provisional_expense_cleanup_v1(
      v_environment,v_cleanup_week_id,p_now_utc
    );
  end loop;

  -- By finalisation time a CONTRACT_EXPENSE workflow already owns an exact
  -- additional Contract week. Reuse that reservation rather than allocating
  -- an unreferenced successor. The active-claim uniqueness constraint normally
  -- makes this one row; fail closed if historical corruption makes it plural.
  select count(*)::integer into v_bound_count
  from public.candidate_submission_workflows workflow
  join public.contract_weeks carrier on carrier.id=workflow.contract_week_id
  where workflow.environment=v_environment
    and workflow.candidate_id=p_candidate_id
    and workflow.contract_id=v_contract.id
    and workflow.week_ending_date=v_anchor_week.week_ending_date
    and workflow.workflow_kind='CONTRACT_EXPENSE'
    and workflow.anchor_timesheet_id=p_anchor_timesheet_id
    and workflow.target_timesheet_id is null
    and ((workflow.route='PAPER' and workflow.state='RECEIVED')
      or (workflow.route<>'PAPER' and workflow.state='READY_TO_FINALISE'))
    and carrier.contract_id=v_contract.id
    and carrier.week_ending_date=v_anchor_week.week_ending_date
    and carrier.additional_seq>0
    and carrier.status='OPEN'
    and carrier.timesheet_id is null;
  if v_bound_count>1 then
    raise exception 'EXPENSE_WORKFLOW_CARRIER_AMBIGUOUS' using errcode='55000';
  elsif v_bound_count=1 then
    select carrier.* into v_bound_week
    from public.candidate_submission_workflows workflow
    join public.contract_weeks carrier on carrier.id=workflow.contract_week_id
    where workflow.environment=v_environment
      and workflow.candidate_id=p_candidate_id
      and workflow.contract_id=v_contract.id
      and workflow.week_ending_date=v_anchor_week.week_ending_date
      and workflow.workflow_kind='CONTRACT_EXPENSE'
      and workflow.anchor_timesheet_id=p_anchor_timesheet_id
      and workflow.target_timesheet_id is null
      and ((workflow.route='PAPER' and workflow.state='RECEIVED')
        or (workflow.route<>'PAPER' and workflow.state='READY_TO_FINALISE'))
      and carrier.contract_id=v_contract.id
      and carrier.week_ending_date=v_anchor_week.week_ending_date
      and carrier.additional_seq>0
      and carrier.status='OPEN'
      and carrier.timesheet_id is null
    limit 1;
    return jsonb_build_object(
      'ok',true,'placement','REUSE_CARRIER','reason_code','WORKFLOW_CARRIER_RESERVED',
      'anchor_timesheet_id',p_anchor_timesheet_id,'anchor_contract_week_id',v_anchor_week.id,
      'target_timesheet_id',null,'target_contract_week_id',v_bound_week.id,
      'target_record_role','FLEXIBLE','idempotent_replay',true,'idempotency_key',p_idempotency_key
    );
  end if;

  v_placement:=public.expense_placement_resolve_v1(p_candidate_id,v_environment,p_anchor_timesheet_id,v_anchor_week.id,'{}'::jsonb,p_now_utc);
  if v_placement->>'placement'='BLOCKED' then
    raise exception '%',v_placement->>'reason_code' using errcode='55000',detail=v_placement::text;
  elsif v_placement->>'placement' in ('SAME_RECORD','REUSE_CARRIER') then
    return v_placement||jsonb_build_object('idempotent_replay',true,'idempotency_key',p_idempotency_key);
  end if;
  select coalesce(max(additional_seq),0)+1 into v_next_seq from public.contract_weeks
  where contract_id=v_contract.id and week_ending_date=v_anchor_week.week_ending_date;
  insert into public.contract_weeks(
    contract_id,week_ending_date,additional_seq,status,submission_mode_snapshot,
    day_entries_json,totals_json,planned_schedule_json,is_adjustment,
    enforce_day_partition,allowed_days_mask,split_boundary_date,split_group_key,
    created_at,updated_at
  ) values (
    v_contract.id,v_anchor_week.week_ending_date,v_next_seq,'OPEN','MANUAL',
    '[]'::jsonb,
    jsonb_build_object(
      'hours',jsonb_build_object('day',0,'night',0,'sat',0,'sun',0,'bh',0),
      'additional_units_week','{}'::jsonb,
      'additional_units_per_day','{}'::jsonb,
      'expenses_draft',jsonb_build_object(
        'mileage_units',0,'travel_pay',0,'travel_charge',0,
        'accommodation_pay',0,'accommodation_charge',0,
        'other_pay',0,'other_charge',0,'note',''
      )
    ),
    '[]'::jsonb,true,
    v_anchor_week.enforce_day_partition,v_anchor_week.allowed_days_mask,
    v_anchor_week.split_boundary_date,v_anchor_week.split_group_key,
    p_now_utc,p_now_utc
  ) returning * into v_new_week;
  perform private._candidate_audit_v1('contract_week',v_new_week.id::text,'CANDIDATE_EXPENSE_CARRIER_CREATED',null,
    jsonb_build_object('contract_id',v_contract.id,'week_ending_date',v_new_week.week_ending_date,'additional_seq',v_new_week.additional_seq),
    null,null,p_idempotency_key,p_now_utc);
  return jsonb_build_object(
    'ok',true,'placement','CREATE_CARRIER','reason_code','CARRIER_CREATED',
    'anchor_timesheet_id',p_anchor_timesheet_id,'anchor_contract_week_id',v_anchor_week.id,
    'target_timesheet_id',null,'target_contract_week_id',v_new_week.id,
    'target_record_role','FLEXIBLE','idempotent_replay',false,'idempotency_key',p_idempotency_key
  );
exception when unique_violation then
  v_placement:=public.expense_placement_resolve_v1(p_candidate_id,v_environment,p_anchor_timesheet_id,v_anchor_week.id,'{}'::jsonb,p_now_utc);
  if v_placement->>'placement'='REUSE_CARRIER' then
    return v_placement||jsonb_build_object('idempotent_replay',true,'idempotency_key',p_idempotency_key);
  end if;
  raise;
end;
$function$;
alter function public.expense_carrier_resolve_or_create_atomic_v1(uuid,text,uuid,text,text,timestamptz) owner to postgres;
revoke all on function public.expense_carrier_resolve_or_create_atomic_v1(uuid,text,uuid,text,text,timestamptz) from public,anon,authenticated;
grant execute on function public.expense_carrier_resolve_or_create_atomic_v1(uuid,text,uuid,text,text,timestamptz) to service_role;


create or replace function public.candidate_office_expense_reservations_v1(
  p_environment text,
  p_actor_user_id uuid,
  p_contract_week_ids uuid[]
)
returns jsonb
language plpgsql
stable
security definer
set search_path = pg_catalog, public, private, pg_temp
as $function$
declare
  v_environment text:=private._candidate_assert_environment(p_environment);
  v_rows jsonb;
begin
  if not exists(select 1 from public.tms_users actor
    where actor.id=p_actor_user_id and actor.is_active) then
    raise exception 'OFFICE_AUTH_REQUIRED' using errcode='28000';
  end if;
  if coalesce(cardinality(p_contract_week_ids),0) not between 1 and 100 then
    raise exception 'CANDIDATE_OFFICE_PROJECTION_BATCH_INVALID' using errcode='22023';
  end if;
  select coalesce(jsonb_agg(jsonb_build_object(
    'contract_week_id',week_row.id,
    'workflow_count',state_row.active_count,
    'state',case when state_row.active_count>1 then 'AMBIGUOUS'
      when state_row.active_count=1 then state_row.active_state
      when state_row.total_count=1 then state_row.only_state
      when state_row.total_count>1 then 'HISTORY' else 'CREATED' end
  ) order by week_row.id),'[]'::jsonb) into v_rows
  from public.contract_weeks week_row
  cross join lateral (
    select count(*) as total_count,min(workflow.state) as only_state,
      count(*) filter(where workflow.state not in ('CANCELLED','EXPIRED','SUPERSEDED','REJECTED','REFUSED')) as active_count,
      min(workflow.state) filter(where workflow.state not in ('CANCELLED','EXPIRED','SUPERSEDED','REJECTED','REFUSED')) as active_state
    from public.candidate_submission_workflows workflow
    where workflow.contract_week_id=week_row.id and workflow.environment=v_environment
      and workflow.workflow_kind='CONTRACT_EXPENSE'
  ) state_row
  where week_row.id=any(p_contract_week_ids)
    and private._candidate_provisional_expense_carrier_v1(week_row.id);
  return jsonb_build_object('ok',true,'rows',v_rows);
end;
$function$;
alter function public.candidate_office_expense_reservations_v1(text,uuid,uuid[]) owner to postgres;
revoke all on function public.candidate_office_expense_reservations_v1(text,uuid,uuid[])
  from public,anon,authenticated;
grant execute on function public.candidate_office_expense_reservations_v1(text,uuid,uuid[]) to service_role;
notify pgrst, 'reload schema';

commit;
