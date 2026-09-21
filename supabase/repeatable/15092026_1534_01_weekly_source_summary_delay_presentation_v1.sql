-- Presentation-only Weekly Source delay fact for Timesheet Summary.
-- It reads the Weekly Source owners and never writes or reclassifies pay,
-- invoice, Workbench, Draft or settlement state.

create or replace function private.weekly_source_summary_pay_delayed_v1(
  p_timesheet_id uuid,
  p_contract_id uuid,
  p_client_id uuid,
  p_week_ending_date date
) returns boolean
language sql
stable
security definer
set search_path to 'public','private','pg_catalog','pg_temp'
as $function$
  with applicable as (
    select cycle.id source_cycle_id,policy.authority_mode
    from public.weekly_source_group_clients membership
    join public.weekly_source_groups source_group
      on source_group.id=membership.source_group_id and source_group.active
    join public.weekly_source_cycles cycle
      on cycle.source_group_id=source_group.id
     and cycle.finalisation_week_ending=p_week_ending_date
    join public.weekly_source_client_policies policy
      on policy.source_group_id=source_group.id and policy.client_id=p_client_id
     and p_week_ending_date between policy.effective_from
       and coalesce(policy.effective_to,'infinity'::date)
    where membership.client_id=p_client_id
      and p_week_ending_date between membership.valid_from
        and coalesce(membership.valid_to,'infinity'::date)
    order by policy.effective_from desc,policy.id desc
    limit 1
  )
  select coalesce((
    select case applicable.authority_mode
      when 'SOURCE_AUTHORITY' then
        not exists(
          select 1 from public.weekly_source_client_cycle_completions completion
          where completion.source_cycle_id=applicable.source_cycle_id
            and completion.client_id=p_client_id and completion.state='CURRENT'
        )
        and not exists(
          select 1
          from public.weekly_exceptional_pay_target_families family
          join public.weekly_exceptional_payment_approvals approval
            on approval.pay_target_family_id=family.id
          where p_timesheet_id is not null
            and family.root_timesheet_id=p_timesheet_id
            and approval.source_cycle_id=applicable.source_cycle_id
            and approval.withdrawn_at_utc is null
            and family.current_lifecycle_state in (
              'PROTECTED','WAITING_SOURCE','READY_TO_RECONCILE','RECONCILED','ACTION_REQUIRED'
            )
        )
      when 'TIMESHEET_AUTHORITY' then
        p_timesheet_id is not null
        and exists(
          select 1
          from public.weekly_timesheet_authority_resolutions authority
          join public.weekly_timesheet_source_comparisons comparison
            on comparison.source_cycle_id=authority.source_cycle_id
           and comparison.contract_id=authority.contract_id
           and comparison.work_date=authority.work_date
          where authority.source_cycle_id=applicable.source_cycle_id
            and authority.contract_id=p_contract_id
            and authority.require_reference_to_pay
            and comparison.timesheet_id=p_timesheet_id
            and comparison.comparison_state<>'EXACT_MATCH'
        )
      else false end
    from applicable
  ),false);
$function$;

alter function private.weekly_source_summary_pay_delayed_v1(uuid,uuid,uuid,date)
  owner to postgres;

comment on function private.weekly_source_summary_pay_delayed_v1(uuid,uuid,uuid,date)
  is 'Presentation-only truth: weekly validation itself currently prevents ordinary candidate pay.';

revoke all on function private.weekly_source_summary_pay_delayed_v1(uuid,uuid,uuid,date)
  from public,anon,authenticated;
grant execute on function private.weekly_source_summary_pay_delayed_v1(uuid,uuid,uuid,date)
  to service_role;
