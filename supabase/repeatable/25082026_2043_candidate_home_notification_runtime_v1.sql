create or replace function private._candidate_home_summary_v1(
  p_environment text,
  p_account_id uuid,
  p_candidate_id uuid,
  p_daily_capability jsonb,
  p_now_utc timestamptz
)
returns jsonb
language plpgsql
stable
security definer
set search_path=''
as $function$
declare
  v_settings public.settings_defaults%rowtype;
  v_unread_count integer:=0;
  v_timesheet_attention_count integer:=0;
  v_next_shift jsonb:=null;
begin
  select * into strict v_settings from public.settings_defaults s where s.id=1;
  if p_account_id is not null then
    select pg_catalog.count(*)::integer into v_unread_count
    from public.candidate_notifications n
    where n.account_id=p_account_id and n.state='UNREAD';
  end if;

  if p_candidate_id is not null then
    select pg_catalog.count(*)::integer into v_timesheet_attention_count
    from public.contract_weeks cw
    join public.contracts c on c.id=cw.contract_id and c.candidate_id=p_candidate_id
    left join public.timesheets t on t.timesheet_id=cw.timesheet_id
      and t.is_current=true and t.archived_at_utc is null
    left join lateral (
      select f.* from public.timesheets_financials f
      where f.timesheet_id=t.timesheet_id and f.is_current=true
      order by f.computed_at_utc desc nulls last,f.updated_at desc,f.id desc limit 1
    ) tf on true
    left join lateral (
      select cs.week_ending_weekday
      from public.client_settings cs
      where cs.client_id=c.client_id
        and cs.effective_from<=(p_now_utc at time zone 'Europe/London')::date
      order by cs.effective_from desc,cs.updated_at desc nulls last,cs.id desc limit 1
    ) effective_client on true
    cross join lateral (
      select (
        (p_now_utc at time zone 'Europe/London')::date
        +pg_catalog.mod(
          coalesce(c.week_ending_weekday_snapshot,effective_client.week_ending_weekday,0)
          -pg_catalog.date_part('dow',(p_now_utc at time zone 'Europe/London')::date)::integer+7,
          7
        )
      )::date as current_week_ending_date
    ) current_window
    cross join lateral (
      select private._candidate_record_capabilities_v1(t.timesheet_id,cw.id,'{}'::jsonb) as value
    ) capability
    where cw.week_ending_date<=current_window.current_week_ending_date
      and tf.paid_at_utc is null
      and coalesce(capability.value->>'record_role','')<>'EXPENSE_ONLY'
      and (
        coalesce((capability.value->>'can_edit_hours')::boolean,false)
        or coalesce((capability.value->>'can_edit_expenses')::boolean,false)
        or exists (
          select 1 from public.candidate_submission_workflows w
          where w.candidate_id=p_candidate_id and w.contract_week_id=cw.id
            and w.state='REJECTED'
            and not private._candidate_rejection_replaced_v1(w.id)
        )
      );

    if coalesce((p_daily_capability->>'enabled')::boolean,false) then
      select pg_catalog.jsonb_build_object(
        'date',d.rota_date,
        'starts_at',d.shift_starts_at,
        'ends_at',d.shift_ends_at,
        'hospital',d.hospital,
        'ward',d.ward,
        'job_title',d.job_title,
        'booking_ref',d.booking_ref
      ) into v_next_shift
      from private.candidate_daily_authority_scopes s
      join public.candidate_daily_rota_generations g
        on g.generation_id=s.active_generation_id and g.state='ACTIVE'
      join public.candidate_daily_rota_days d on d.generation_id=g.generation_id
      where s.environment=pg_catalog.upper(p_environment)
        and s.candidate_id=p_candidate_id
        and d.booked and d.shift_starts_at is not null and d.shift_ends_at>p_now_utc
      order by d.shift_starts_at,d.rota_date,d.booking_id
      limit 1;
    end if;
  end if;

  return pg_catalog.jsonb_build_object(
    'announcement',pg_catalog.jsonb_build_object(
      'text',v_settings.candidate_home_announcement_text,
      'version',v_settings.candidate_home_announcement_version,
      'updated_at_utc',v_settings.candidate_home_announcement_updated_at_utc
    ),
    'timesheets',pg_catalog.jsonb_build_object(
      'attention_count',v_timesheet_attention_count
    ),
    'notifications',pg_catalog.jsonb_build_object('unread_count',v_unread_count),
    'next_shift',v_next_shift
  );
end;
$function$;

alter function private._candidate_home_summary_v1(text,uuid,uuid,jsonb,timestamptz)
  owner to postgres;
revoke all on function private._candidate_home_summary_v1(text,uuid,uuid,jsonb,timestamptz)
  from public,anon,authenticated,service_role;
