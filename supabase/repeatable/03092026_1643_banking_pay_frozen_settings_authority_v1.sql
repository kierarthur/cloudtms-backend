\set ON_ERROR_STOP on

begin;

-- These two Banking projections contain mature payment calculations that are
-- deliberately left byte-for-byte unchanged. Replace only their two live
-- Client/Contract reads for require_reference_to_pay with the immutable
-- authority already frozen on each real Timesheet. Each replacement is
-- cardinality-gated so source drift fails the release closed.
do $banking_pay_frozen_settings_authority_v1$
declare
  v_definition text;
  v_old text;
  v_new text;
  v_matches integer;
  v_new_matches integer;
  v_changed boolean;
begin
  select pg_catalog.pg_get_functiondef(
    pg_catalog.to_regprocedure('public.pay_timesheet_impact_preview(uuid)')
  ) into v_definition;
  if v_definition is null then
    raise exception 'BANKING_PAY_TIMESHEET_IMPACT_DEFINITION_MISSING';
  end if;

  v_old := $old$
  select
    coalesce(
      case when ct.overrideclientsettings then ct.require_reference_to_pay end,
      cs.pay_reference_required,
      false
    )
  into v_require_ref
  from public.contracts ct
  left join public.client_settings cs on cs.client_id = v_tf.client_id
  where ct.id = v_ts.contract_id
  limit 1;$old$;
  v_new := $new$
  select coalesce(
    (private._timesheet_settings_authority_frozen_v1(p_timesheet_id)
      #>>'{values,require_reference_to_pay}')::boolean,
    false
  )
  into v_require_ref;$new$;
  v_matches := (length(v_definition)-length(replace(v_definition,v_old,'')))
    / nullif(length(v_old),0);
  v_new_matches := (length(v_definition)-length(replace(v_definition,v_new,'')))
    / nullif(length(v_new),0);
  if v_matches=1 and v_new_matches=0 then
    execute replace(v_definition,v_old,v_new);
  elsif v_matches<>0 or v_new_matches<>1 then
    raise exception 'BANKING_PAY_TIMESHEET_IMPACT_SOURCE_DRIFT:%',v_matches;
  end if;

  select pg_catalog.pg_get_functiondef(
    pg_catalog.to_regprocedure(
      'public.pay_preview_candidate_collect_scope(jsonb,uuid,jsonb,integer)'
    )
  ) into v_definition;
  if v_definition is null then
    raise exception 'BANKING_PAY_COLLECT_SCOPE_DEFINITION_MISSING';
  end if;

  v_old := $old$case when coalesce(con.overrideclientsettings,false) = true then coalesce(con.require_reference_to_pay,false) else coalesce(cs.pay_reference_required,false) end as require_reference_to_pay,$old$;
  v_new := $new$coalesce((private._timesheet_settings_authority_frozen_v1(ts.timesheet_id)#>>'{values,require_reference_to_pay}')::boolean,false) as require_reference_to_pay,$new$;
  v_matches := (length(v_definition)-length(replace(v_definition,v_old,'')))
    / nullif(length(v_old),0);
  v_new_matches := (length(v_definition)-length(replace(v_definition,v_new,'')))
    / nullif(length(v_new),0);
  if v_matches=2 and v_new_matches=0 then
    execute replace(v_definition,v_old,v_new);
  elsif v_matches<>0 or v_new_matches<>2 then
    raise exception 'BANKING_PAY_COLLECT_SCOPE_SOURCE_DRIFT:%',v_matches;
  end if;

  -- The Office summary's mature definition is also retained in its historical
  -- owner file. Remove only the all-history Client reference fallback. The
  -- existing per-row fields already come from the planned-week or frozen
  -- Timesheet authority, so every stage and result column remains unchanged.
  select pg_catalog.pg_get_functiondef(
    pg_catalog.to_regprocedure(
      'public.timesheet_summary_lightweight_rows_v1(jsonb)'
    )
  ) into v_definition;
  if v_definition is null then
    raise exception 'OFFICE_TIMESHEET_SUMMARY_DEFINITION_MISSING';
  end if;
  v_changed:=false;

  v_old := $old$  client_reference_settings AS MATERIALIZED (
    SELECT
      client_setting.client_id,
      COALESCE(BOOL_OR(client_setting.reference_number_required_to_issue_invoice), FALSE)
        AS issue_reference_required
    FROM public.client_settings AS client_setting
    WHERE EXISTS (
      SELECT 1
      FROM source_rows
      WHERE source_rows.client_id = client_setting.client_id
    )
    GROUP BY client_setting.client_id
  ),
$old$;
  v_matches := (length(v_definition)-length(replace(v_definition,v_old,'')))
    / nullif(length(v_old),0);
  if v_matches=1 then
    v_definition:=replace(v_definition,v_old,'');
    v_changed:=true;
  elsif v_matches<>0 then
    raise exception 'OFFICE_TIMESHEET_SUMMARY_CTE_SOURCE_DRIFT:%',v_matches;
  end if;

  v_old := $old$            OR COALESCE(client_reference_settings.issue_reference_required, FALSE)
$old$;
  v_matches := (length(v_definition)-length(replace(v_definition,v_old,'')))
    / nullif(length(v_old),0);
  if v_matches=1 then
    v_definition:=replace(v_definition,v_old,'');
    v_changed:=true;
  elsif v_matches<>0 then
    raise exception 'OFFICE_TIMESHEET_SUMMARY_POLICY_SOURCE_DRIFT:%',v_matches;
  end if;

  v_old := $old$    LEFT JOIN client_reference_settings
      ON client_reference_settings.client_id = source_rows.client_id
$old$;
  v_matches := (length(v_definition)-length(replace(v_definition,v_old,'')))
    / nullif(length(v_old),0);
  if v_matches=1 then
    v_definition:=replace(v_definition,v_old,'');
    v_changed:=true;
  elsif v_matches<>0 then
    raise exception 'OFFICE_TIMESHEET_SUMMARY_JOIN_SOURCE_DRIFT:%',v_matches;
  end if;

  if v_definition like '%client_reference_settings%'
     or v_definition like '%BOOL_OR(client_setting.reference_number_required_to_issue_invoice)%' then
    raise exception 'OFFICE_TIMESHEET_SUMMARY_LIVE_HISTORY_READ_REMAINS';
  end if;
  if v_changed then
    execute v_definition;
  end if;
end
$banking_pay_frozen_settings_authority_v1$;

commit;
