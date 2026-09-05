-- Candidate weekly read fast path.
--
-- The former route resolver called _candidate_import_authoritative_v1 for
-- every card. That helper reloaded the same Timesheet, Contract, financials
-- and settings authority and then queried the complete v_timesheets_summary
-- view for one route value. Home, MyTMS Timesheet List and Office projections
-- all call this resolver once or more per row.
--
-- Preserve the Daily path and the complete response contract. For Weekly
-- records, derive the same route fact from the rows already loaded here and
-- the same canonical settings authority, without recursively reading the
-- complete Office summary view.

\set ON_ERROR_STOP on

begin;

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
    'candidate_hours_submission_allowed',v_family='ELECTRONIC'
      or (v_family='QR' and not v_is_daily),
    'candidate_expenses_allowed',v_family in (
      'ELECTRONIC','QR','IMPORT_AUTHORITATIVE'
    ),
    'candidate_paper_submission_allowed',not v_is_daily
      and (v_family='QR' or (v_family='ELECTRONIC' and v_paper_fallback)),
    'candidate_no_work_allowed',v_family='ELECTRONIC'
      or (v_family='QR' and not v_is_daily),
    'policy',v_policy
  );
end;
$function$;

alter function private._candidate_route_family_v1(uuid,uuid) owner to postgres;
revoke all on function private._candidate_route_family_v1(uuid,uuid)
  from public,anon,authenticated,service_role;

comment on function private._candidate_route_family_v1(uuid,uuid) is
  'Candidate route authority. Weekly reads reuse loaded canonical facts and never recursively query the complete Office Timesheet Summary view; Daily behaviour is unchanged.';

commit;
