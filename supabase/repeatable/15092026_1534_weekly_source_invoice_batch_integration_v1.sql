-- Repeatable CloudTMS authority: weekly_source_invoice_batch_integration_v1
--
-- Narrowly joins finalised Weekly source manifests to the existing invoice
-- batch and invoice-detail surfaces.  It does not discover ordinary
-- Timesheets, create pay, or change Workbench / Banking Pay state.

\set ON_ERROR_STOP on

begin;

create or replace function private.weekly_source_invoice_batch_snapshot_v1()
returns text
language sql
stable
security definer
set search_path to 'public','private','pg_catalog','pg_temp'
as $function$
  select pg_catalog.encode(
    private.weekly_source_sha256_jsonb_v1(
      'WEEKLY_SOURCE_INVOICE_BATCH_SNAPSHOT_V1',
      coalesce(pg_catalog.jsonb_agg(f.fact order by f.manifest_id),'[]'::jsonb)
    ),
    'hex'
  )
  from (
    select manifest.id manifest_id,
      pg_catalog.jsonb_build_object(
        'manifest_id',manifest.id,
        'manifest_hash',pg_catalog.encode(manifest.manifest_hash,'hex'),
        'invoice_state',manifest.invoice_state,
        'movement_count',manifest.movement_count,
        'revision_state',revision.state,
        'completion_id',completion.id,
        'members',coalesce((
          select pg_catalog.jsonb_agg(pg_catalog.jsonb_build_object(
            'movement_id',movement.id,
            'movement_hash',pg_catalog.encode(movement.movement_economic_hash,'hex'),
            'placement_state',movement.placement_state,
            'binding_invoice_id',binding.invoice_id
          ) order by member.manifest_ordinal)
          from public.weekly_source_manifest_movements member
          join public.weekly_source_billing_movements movement
            on movement.id=member.billing_movement_id
          left join public.weekly_source_invoice_line_bindings binding
            on binding.billing_movement_id=movement.id and binding.state='CURRENT'
          where member.client_manifest_id=manifest.id
        ),'[]'::jsonb)
      ) fact
    from public.weekly_source_client_manifests manifest
    join public.weekly_source_final_revisions revision
      on revision.id=manifest.final_revision_id
    join public.weekly_source_client_cycle_completions completion
      on completion.source_cycle_id=manifest.source_cycle_id
     and completion.client_id=manifest.client_id
     and completion.final_revision_id=manifest.final_revision_id
     and completion.completion_kind='FINAL_SOURCE'
     and completion.state='CURRENT'
    where manifest.invoice_state='READY'
      and manifest.movement_count>0
      and revision.state='CURRENT'
  ) f;
$function$;

create or replace function private.weekly_source_invoice_batch_rows_v1(
  p_filters jsonb default '{}'::jsonb,
  p_selection jsonb default pg_catalog.jsonb_build_object(
    'contract_version','INVOICE_BATCH_SELECTION_V2',
    'mode','IMPLICIT_ALL','default_selected',true,'rules','[]'::jsonb
  )
) returns table(
  manifest_id uuid,
  selection_key text,
  source_revision text,
  source_group_id uuid,
  source_cycle_id uuid,
  client_id uuid,
  client_name text,
  finalisation_week_ending date,
  backing_report_number text,
  movement_count integer,
  candidate_ids uuid[],
  candidate_names text[],
  total_ex_vat numeric,
  vat_amount numeric,
  total_inc_vat numeric,
  released_after_dispute boolean,
  selectable boolean,
  blocker_codes text[],
  selected boolean
)
language sql
stable
security definer
set search_path to 'public','private','pg_catalog','pg_temp'
as $function$
  with rules as materialized (
    select * from private._invoice_batch_selection_rules_v2(p_selection)
  ),
  raw as materialized (
    select
      manifest.id manifest_id,
      'weekly-source-manifest:'||manifest.id::text selection_key,
      pg_catalog.encode(manifest.manifest_hash,'hex') source_revision,
      manifest.source_group_id,
      manifest.source_cycle_id,
      manifest.client_id,
      coalesce(nullif(pg_catalog.btrim(client.name),''),manifest.client_id::text) client_name,
      manifest.finalisation_week_ending,
      nullif(pg_catalog.btrim(manifest.backing_report_number),'') backing_report_number,
      manifest.movement_count,
      coalesce(pg_catalog.array_agg(distinct movement.candidate_id order by movement.candidate_id)
        filter(where movement.candidate_id is not null),'{}'::uuid[]) candidate_ids,
      coalesce(pg_catalog.array_agg(distinct coalesce(
        nullif(pg_catalog.btrim(candidate.display_name),''),
        nullif(pg_catalog.btrim(pg_catalog.concat_ws(' ',candidate.first_name,candidate.last_name)),''),
        candidate.tms_ref,movement.candidate_id::text
      ) order by coalesce(
        nullif(pg_catalog.btrim(candidate.display_name),''),
        nullif(pg_catalog.btrim(pg_catalog.concat_ws(' ',candidate.first_name,candidate.last_name)),''),
        candidate.tms_ref,movement.candidate_id::text
      )) filter(where movement.candidate_id is not null),'{}'::text[]) candidate_names,
      coalesce(pg_catalog.sum(movement.invoice_presentation_charge_pence),0)::numeric/100 total_ex_vat,
      coalesce(pg_catalog.sum(movement.vat_amount),0) vat_amount,
      coalesce(pg_catalog.sum(movement.total_inc_vat),0) total_inc_vat,
      coalesce(pg_catalog.bool_or(exists(
        select 1 from public.weekly_discrepancy_incidents incident
        where incident.source_group_id=manifest.source_group_id
          and incident.work_event_id=movement.work_event_id
      )),false) released_after_dispute,
      (
        manifest.movement_count>0
        and pg_catalog.count(member.billing_movement_id)=manifest.movement_count
        and pg_catalog.bool_and(member.movement_hash=movement.movement_economic_hash)
        and pg_catalog.bool_and(movement.final_revision_id=manifest.final_revision_id)
        and pg_catalog.bool_and(movement.finalisation_cycle_id=manifest.source_cycle_id)
        and pg_catalog.bool_and(movement.actual_client_id=manifest.client_id)
        and pg_catalog.bool_and(movement.placement_state='UNPLACED')
        and pg_catalog.bool_and(binding.id is null)
      ) integrity_ok
    from public.weekly_source_client_manifests manifest
    join public.weekly_source_final_revisions revision
      on revision.id=manifest.final_revision_id and revision.state='CURRENT'
    join public.weekly_source_client_cycle_completions completion
      on completion.source_cycle_id=manifest.source_cycle_id
     and completion.client_id=manifest.client_id
     and completion.final_revision_id=manifest.final_revision_id
     and completion.completion_kind='FINAL_SOURCE' and completion.state='CURRENT'
    join public.clients client on client.id=manifest.client_id
    left join public.weekly_source_manifest_movements member
      on member.client_manifest_id=manifest.id
    left join public.weekly_source_billing_movements movement
      on movement.id=member.billing_movement_id
    left join public.candidates candidate on candidate.id=movement.candidate_id
    left join public.weekly_source_invoice_line_bindings binding
      on binding.billing_movement_id=movement.id and binding.state='CURRENT'
    where manifest.invoice_state='READY'
      and manifest.movement_count>0
    group by manifest.id,client.name
  ),
  filtered as materialized (
    select raw.*,
      case when raw.integrity_ok then '{}'::text[]
        else array['SOURCE_MANIFEST_INVALID']::text[] end blocker_codes
    from raw
    where
      (pg_catalog.jsonb_array_length(coalesce(p_filters->'client_ids','[]'::jsonb))=0
        or raw.client_id::text in (select pg_catalog.jsonb_array_elements_text(p_filters->'client_ids')))
      and (pg_catalog.jsonb_array_length(coalesce(p_filters->'candidate_ids','[]'::jsonb))=0
        or exists(select 1 from pg_catalog.unnest(raw.candidate_ids) candidate_id
          where candidate_id::text in (select pg_catalog.jsonb_array_elements_text(p_filters->'candidate_ids'))))
      and (pg_catalog.jsonb_array_length(coalesce(p_filters->'week_endings','[]'::jsonb))=0
        or raw.finalisation_week_ending::text in (select pg_catalog.jsonb_array_elements_text(p_filters->'week_endings')))
      and (nullif(p_filters->>'week_ending_from','') is null
        or raw.finalisation_week_ending>=nullif(p_filters->>'week_ending_from','')::date)
      and (nullif(p_filters->>'week_ending_to','') is null
        or raw.finalisation_week_ending<=nullif(p_filters->>'week_ending_to','')::date)
      and (pg_catalog.jsonb_array_length(coalesce(p_filters->'status_codes','[]'::jsonb))=0
        or case when raw.integrity_ok then 'READY' else 'BLOCKED' end
          in (select pg_catalog.upper(pg_catalog.jsonb_array_elements_text(p_filters->'status_codes'))))
      and (coalesce(pg_catalog.upper(nullif(p_filters->>'display_mode','')),'ALL')='ALL'
        or coalesce(pg_catalog.upper(nullif(p_filters->>'display_mode','')),'ALL')=
          case when raw.integrity_ok then 'READY' else 'BLOCKED' end)
      and (pg_catalog.jsonb_array_length(coalesce(p_filters->'blocker_codes','[]'::jsonb))=0
        or (not raw.integrity_ok and 'SOURCE_MANIFEST_INVALID'
          in (select pg_catalog.upper(pg_catalog.jsonb_array_elements_text(p_filters->'blocker_codes')))))
      and (nullif(pg_catalog.lower(pg_catalog.btrim(p_filters->>'search')),'') is null
        or pg_catalog.lower(raw.client_name||' '||coalesce(raw.backing_report_number,'')||' '
          ||pg_catalog.array_to_string(raw.candidate_names,' ')) like
          '%'||pg_catalog.lower(pg_catalog.btrim(p_filters->>'search'))||'%')
  ),
  decided as (
    select filtered.*,
      coalesce((
        select rule.action='INCLUDE'
        from rules rule
        where case rule.selector_type
          when 'ROW' then rule.selection_key=filtered.selection_key
          when 'WEEK' then rule.week_ending_date=filtered.finalisation_week_ending
          when 'CLIENT' then rule.client_id=filtered.client_id
          when 'CANDIDATE' then rule.candidate_id=any(filtered.candidate_ids)
          when 'STATUS' then rule.status_code=case when filtered.integrity_ok then 'READY' else 'BLOCKED' end
          when 'WEEK_CLIENT' then rule.week_ending_date=filtered.finalisation_week_ending and rule.client_id=filtered.client_id
          when 'WEEK_CLIENT_CANDIDATE' then rule.week_ending_date=filtered.finalisation_week_ending
            and rule.client_id=filtered.client_id and rule.candidate_id=any(filtered.candidate_ids)
          when 'STATUS_WEEK' then rule.status_code=case when filtered.integrity_ok then 'READY' else 'BLOCKED' end
            and rule.week_ending_date=filtered.finalisation_week_ending
          when 'STATUS_WEEK_CLIENT' then rule.status_code=case when filtered.integrity_ok then 'READY' else 'BLOCKED' end
            and rule.week_ending_date=filtered.finalisation_week_ending and rule.client_id=filtered.client_id
          when 'DIMENSION_GROUP' then (rule.week_ending_date is null or rule.week_ending_date=filtered.finalisation_week_ending)
            and (rule.client_id is null or rule.client_id=filtered.client_id)
            and (rule.candidate_id is null or rule.candidate_id=any(filtered.candidate_ids))
            and (rule.status_code is null or rule.status_code=case when filtered.integrity_ok then 'READY' else 'BLOCKED' end)
          else false end
        order by rule.rule_sequence desc limit 1
      ),true) and filtered.integrity_ok is_selected
    from filtered
  )
  select decided.manifest_id,decided.selection_key,decided.source_revision,
    decided.source_group_id,decided.source_cycle_id,decided.client_id,
    decided.client_name,decided.finalisation_week_ending,
    decided.backing_report_number,decided.movement_count,
    decided.candidate_ids,decided.candidate_names,decided.total_ex_vat,
    decided.vat_amount,decided.total_inc_vat,decided.released_after_dispute,
    decided.integrity_ok,decided.blocker_codes,decided.is_selected
  from decided;
$function$;

create or replace function public.weekly_source_invoice_batch_candidates_v1(
  p_request jsonb
) returns jsonb
language plpgsql
stable
security definer
set search_path to 'public','private','pg_catalog','pg_temp'
as $function$
declare
  v_allowed constant text[]:=array[
    'mode','snapshot_hash','filters','sort','selection','page_size',
    'selection_keys','expected_source_revisions'
  ];
  v_unknown text;
  v_mode text;
  v_snapshot text;
  v_current_snapshot text;
  v_filters jsonb;
  v_selection jsonb;
  v_page_size integer;
  v_rows jsonb:='[]'::jsonb;
  v_total integer:=0;
  v_eligible integer:=0;
  v_selected integer:=0;
  v_blocked integer:=0;
  v_refs jsonb:='[]'::jsonb;
  v_selection_keys jsonb;
  v_expected jsonb;
begin
  perform private.weekly_source_query_require_service_v1();
  if p_request is null or pg_catalog.jsonb_typeof(p_request)<>'object' then
    raise exception 'WEEKLY_SOURCE_INVOICE_BATCH_REQUEST_INVALID' using errcode='22023';
  end if;
  select key into v_unknown from pg_catalog.jsonb_object_keys(p_request) key
  where not key=any(v_allowed) order by key limit 1;
  if v_unknown is not null then
    raise exception 'WEEKLY_SOURCE_INVOICE_BATCH_UNKNOWN_FIELD' using errcode='22023',detail=v_unknown;
  end if;
  v_mode:=pg_catalog.upper(coalesce(nullif(pg_catalog.btrim(p_request->>'mode'),''),'PAGE'));
  if v_mode not in ('PAGE','SUMMARY','FACETS','EXPLICIT_KEYS','CONFIRM') then
    raise exception 'WEEKLY_SOURCE_INVOICE_BATCH_MODE_INVALID' using errcode='22023';
  end if;
  v_filters:=coalesce(p_request->'filters','{}'::jsonb);
  v_selection:=coalesce(p_request->'selection',pg_catalog.jsonb_build_object(
    'contract_version','INVOICE_BATCH_SELECTION_V2','mode','IMPLICIT_ALL',
    'default_selected',true,'rules','[]'::jsonb));
  if pg_catalog.jsonb_typeof(v_filters)<>'object' or pg_catalog.jsonb_typeof(v_selection)<>'object' then
    raise exception 'WEEKLY_SOURCE_INVOICE_BATCH_REQUEST_INVALID' using errcode='22023';
  end if;
  perform 1 from private._invoice_batch_selection_rules_v2(v_selection) limit 1;
  v_page_size:=coalesce(nullif(p_request->>'page_size','')::integer,100);
  if v_page_size not between 1 and 5000 then
    raise exception 'WEEKLY_SOURCE_INVOICE_BATCH_PAGE_SIZE_INVALID' using errcode='22023';
  end if;
  v_current_snapshot:=private.weekly_source_invoice_batch_snapshot_v1();
  v_snapshot:=nullif(pg_catalog.lower(pg_catalog.btrim(p_request->>'snapshot_hash')),'');
  if v_snapshot is not null and (v_snapshot!~'^[0-9a-f]{64}$' or v_snapshot<>v_current_snapshot) then
    raise exception 'BATCH_SOURCE_CHANGED' using errcode='40001';
  end if;
  if v_mode<>'PAGE' and v_snapshot is null then
    raise exception 'BATCH_SNAPSHOT_REQUIRED' using errcode='22023';
  end if;

  v_selection_keys:=coalesce(p_request->'selection_keys','[]'::jsonb);
  v_expected:=coalesce(p_request->'expected_source_revisions','{}'::jsonb);
  if v_mode in ('EXPLICIT_KEYS','CONFIRM') and (
    pg_catalog.jsonb_typeof(v_selection_keys)<>'array'
    or pg_catalog.jsonb_typeof(v_expected)<>'object'
  ) then raise exception 'BATCH_EXPLICIT_KEYS_INVALID' using errcode='22023'; end if;
  if v_mode='EXPLICIT_KEYS' and pg_catalog.jsonb_array_length(v_selection_keys)<>1 then
    raise exception 'BATCH_EXPLICIT_KEYS_INVALID' using errcode='22023';
  end if;

  with candidate as materialized (
    select * from private.weekly_source_invoice_batch_rows_v1(v_filters,v_selection)
  ), scoped as materialized (
    select candidate.* from candidate
    where v_mode not in ('EXPLICIT_KEYS','CONFIRM')
      or (v_mode='CONFIRM' and pg_catalog.jsonb_array_length(v_selection_keys)=0)
      or candidate.selection_key in (select pg_catalog.jsonb_array_elements_text(v_selection_keys))
  )
  select pg_catalog.count(*)::integer,
    pg_catalog.count(*) filter(where selectable)::integer,
    pg_catalog.count(*) filter(where selected)::integer,
    pg_catalog.count(*) filter(where not selectable)::integer
  into v_total,v_eligible,v_selected,v_blocked from scoped;

  if (v_mode='PAGE' and v_total>v_page_size)
     or (v_mode='CONFIRM' and v_selected>5000) then
    raise exception 'WEEKLY_SOURCE_INVOICE_BATCH_SCOPE_TOO_LARGE'
      using errcode='54000';
  end if;

  if v_mode='EXPLICIT_KEYS'
     or (v_mode='CONFIRM' and pg_catalog.jsonb_array_length(v_selection_keys)>0) then
    if (select pg_catalog.count(*) from pg_catalog.jsonb_array_elements_text(v_selection_keys))<>v_total
       or exists(
         select 1 from private.weekly_source_invoice_batch_rows_v1(v_filters,v_selection) row_value
         where row_value.selection_key in (select pg_catalog.jsonb_array_elements_text(v_selection_keys))
           and coalesce(v_expected->>row_value.selection_key,'')<>row_value.source_revision
       ) then
      raise exception 'BATCH_SOURCE_CHANGED' using errcode='40001';
    end if;
  end if;

  if v_mode in ('PAGE','EXPLICIT_KEYS') then
    select coalesce(pg_catalog.jsonb_agg(pg_catalog.jsonb_build_object(
      'selection_key',row_value.selection_key,
      'source_revision',row_value.source_revision,
      'source_kind','WEEKLY_FINAL_SOURCE',
      'invoice_stream','WEEKLY_FINAL_SOURCE',
      'selectable',row_value.selectable,
      'row_status',case when row_value.selectable then 'READY' else 'BLOCKED' end,
      'generation_state','NOT_GENERATED',
      'week_ending_date',row_value.finalisation_week_ending,
      'client_id',row_value.client_id,'client_name',row_value.client_name,
      'candidate_ids',pg_catalog.to_jsonb(row_value.candidate_ids),
      'candidate_names',pg_catalog.to_jsonb(row_value.candidate_names),
      'candidate_name',case pg_catalog.cardinality(row_value.candidate_ids)
        when 1 then row_value.candidate_names[1]
        else pg_catalog.cardinality(row_value.candidate_ids)::text||' workers' end,
      'report_number',row_value.backing_report_number,
      'movement_count',row_value.movement_count,
      'total_ex_vat',row_value.total_ex_vat,'vat_amount',row_value.vat_amount,
      'total_inc_vat',row_value.total_inc_vat,'currency','GBP',
      'action_blocker_codes',pg_catalog.to_jsonb(row_value.blocker_codes),
      'informational_codes',case when row_value.released_after_dispute
        then '["RELEASED_AFTER_DISPUTE"]'::jsonb else '[]'::jsonb end,
      'released_after_dispute',row_value.released_after_dispute,
      'client_manifest_id',row_value.manifest_id,
      'source_cycle_id',row_value.source_cycle_id,
      'command_payload',pg_catalog.jsonb_build_object(
        'command_type','ADMIT_WEEKLY_SOURCE_MANIFEST',
        'client_manifest_id',row_value.manifest_id,
        'expected_manifest_hash',row_value.source_revision
      )
    ) order by row_value.finalisation_week_ending desc,row_value.client_name,row_value.manifest_id),'[]'::jsonb)
    into v_rows
    from (
      select * from private.weekly_source_invoice_batch_rows_v1(v_filters,v_selection)
      where (v_mode<>'EXPLICIT_KEYS' or selection_key in (
        select pg_catalog.jsonb_array_elements_text(v_selection_keys)))
      order by finalisation_week_ending desc,client_name,manifest_id limit v_page_size
    ) row_value;
  end if;

  if v_mode='CONFIRM' then
    select coalesce(pg_catalog.jsonb_agg(pg_catalog.jsonb_build_object(
      'client_manifest_id',row_value.manifest_id,
      'expected_manifest_hash',row_value.source_revision,
      'selection_key',row_value.selection_key,
      'client_id',row_value.client_id,
      'source_cycle_id',row_value.source_cycle_id,
      'finalisation_week_ending',row_value.finalisation_week_ending,
      'report_number',row_value.backing_report_number
    ) order by row_value.finalisation_week_ending,row_value.client_id,row_value.manifest_id),'[]'::jsonb)
    into v_refs
    from private.weekly_source_invoice_batch_rows_v1(v_filters,v_selection) row_value
    where row_value.selected
      and (pg_catalog.jsonb_array_length(v_selection_keys)=0
        or row_value.selection_key in (
          select pg_catalog.jsonb_array_elements_text(v_selection_keys)));
  end if;

  return pg_catalog.jsonb_build_object(
    'contract_version','WEEKLY_SOURCE_INVOICE_BATCH_CANDIDATES_V1',
    'mode',v_mode,'snapshot_hash',v_current_snapshot,'rows',v_rows,
    'page',pg_catalog.jsonb_build_object('total_count',v_total,'returned_count',pg_catalog.jsonb_array_length(v_rows)),
    'selection_summary',pg_catalog.jsonb_build_object(
      'exact',true,'eligible_total',v_eligible,'selected_total',v_selected,'blocked_total',v_blocked),
    'selected_manifest_refs',v_refs
  );
end;
$function$;

create or replace function public.weekly_source_invoice_batch_admit_atomic_v1(
  p_request jsonb
) returns jsonb
language plpgsql
security definer
set search_path to 'public','private','pg_catalog','pg_temp'
as $function$
declare
  v_actor uuid;
  v_snapshot text;
  v_current_snapshot text;
  v_token text;
  v_refs jsonb;
  v_ref jsonb;
  v_manifest public.weekly_source_client_manifests%rowtype;
  v_all_admitted boolean;
  v_result jsonb;
  v_results jsonb:='[]'::jsonb;
begin
  perform private.weekly_source_query_require_service_v1();
  if p_request is null or pg_catalog.jsonb_typeof(p_request)<>'object'
     or exists(select 1 from pg_catalog.jsonb_object_keys(p_request) key
       where key not in ('actor_user_id','command_token','snapshot_hash','selected_manifests')) then
    raise exception 'WEEKLY_SOURCE_INVOICE_BATCH_ADMIT_REQUEST_INVALID' using errcode='22023';
  end if;
  begin v_actor:=(p_request->>'actor_user_id')::uuid;
  exception when others then
    raise exception 'WEEKLY_SOURCE_INVOICE_BATCH_ADMIT_REQUEST_INVALID' using errcode='22023';
  end;
  v_token:=nullif(pg_catalog.btrim(p_request->>'command_token'),'');
  v_snapshot:=nullif(pg_catalog.lower(pg_catalog.btrim(p_request->>'snapshot_hash')),'');
  v_refs:=coalesce(p_request->'selected_manifests','[]'::jsonb);
  if v_actor is null or v_token is null or pg_catalog.length(v_token)>256
     or v_snapshot!~'^[0-9a-f]{64}$'
     or pg_catalog.jsonb_typeof(v_refs)<>'array'
     or pg_catalog.jsonb_array_length(v_refs) not between 1 and 5000
     or exists(select 1 from pg_catalog.jsonb_array_elements(v_refs) item
       where pg_catalog.jsonb_typeof(item)<>'object'
         or exists(select 1 from pg_catalog.jsonb_object_keys(item) key
           where key not in ('client_manifest_id','expected_manifest_hash','selection_key',
             'client_id','source_cycle_id','finalisation_week_ending','report_number'))
         or not pg_catalog.pg_input_is_valid(coalesce(item->>'client_manifest_id',''),'uuid')
         or coalesce(item->>'selection_key','')<>
           'weekly-source-manifest:'||coalesce(item->>'client_manifest_id','')
         or coalesce(item->>'expected_manifest_hash','')!~'^[0-9a-f]{64}$')
     or (select pg_catalog.count(distinct item->>'client_manifest_id')
         from pg_catalog.jsonb_array_elements(v_refs) item)<>pg_catalog.jsonb_array_length(v_refs) then
    raise exception 'WEEKLY_SOURCE_INVOICE_BATCH_ADMIT_REQUEST_INVALID' using errcode='22023';
  end if;

  select pg_catalog.bool_and(manifest.invoice_state='ADMITTED'
    and pg_catalog.encode(manifest.manifest_hash,'hex')=item->>'expected_manifest_hash')
  into v_all_admitted
  from pg_catalog.jsonb_array_elements(v_refs) item
  join public.weekly_source_client_manifests manifest
    on manifest.id=(item->>'client_manifest_id')::uuid;
  v_all_admitted:=coalesce(v_all_admitted,false);
  v_current_snapshot:=private.weekly_source_invoice_batch_snapshot_v1();
  if not v_all_admitted and v_current_snapshot<>v_snapshot then
    raise exception 'BATCH_SOURCE_CHANGED' using errcode='40001';
  end if;

  for v_ref in select item from pg_catalog.jsonb_array_elements(v_refs) item
    order by item->>'finalisation_week_ending',item->>'client_id',item->>'client_manifest_id'
  loop
    select * into v_manifest from public.weekly_source_client_manifests
    where id=(v_ref->>'client_manifest_id')::uuid for update;
    if not found or pg_catalog.encode(v_manifest.manifest_hash,'hex')<>v_ref->>'expected_manifest_hash'
       or v_manifest.invoice_state not in ('READY','ADMITTED') then
      raise exception 'BATCH_SOURCE_CHANGED' using errcode='40001';
    end if;
    v_result:=public.weekly_source_invoice_admit_atomic_v1(pg_catalog.jsonb_build_object(
      'actor_user_id',v_actor,'client_manifest_id',v_manifest.id,
      'expected_manifest_hash',v_ref->>'expected_manifest_hash'
    ));
    v_results:=v_results||pg_catalog.jsonb_build_array(
      pg_catalog.jsonb_build_object(
        'selection_key',v_ref->>'selection_key',
        'client_manifest_id',v_manifest.id,
        'client_id',v_manifest.client_id,
        'source_cycle_id',v_manifest.source_cycle_id,
        'finalisation_week_ending',v_manifest.finalisation_week_ending,
        'report_number',v_manifest.backing_report_number,
        'status',v_result->>'status','idempotent',coalesce((v_result->>'idempotent')::boolean,false),
        'invoice_ids',coalesce(v_result->'invoice_ids','[]'::jsonb)
      )
    );
  end loop;

  if exists(
    select 1
    from pg_catalog.jsonb_array_elements(v_results) left_result
    join pg_catalog.jsonb_array_elements(v_results) right_result
      on left_result->>'client_manifest_id'<right_result->>'client_manifest_id'
     and left_result->>'client_id'=right_result->>'client_id'
     and left_result->>'source_cycle_id'<>right_result->>'source_cycle_id'
    where exists(
      select 1 from pg_catalog.jsonb_array_elements_text(left_result->'invoice_ids') left_invoice
      join pg_catalog.jsonb_array_elements_text(right_result->'invoice_ids') right_invoice
        on left_invoice=right_invoice
    )
  ) then
    raise exception 'WEEKLY_SOURCE_INVOICE_CYCLE_CONSOLIDATION_REFUSED' using errcode='55000';
  end if;

  return pg_catalog.jsonb_build_object(
    'ok',true,'contract_version','WEEKLY_SOURCE_INVOICE_BATCH_ADMISSION_V1',
    'command_token',v_token,'atomic',true,
    'selected_count',pg_catalog.jsonb_array_length(v_results),
    'per_manifest_results',v_results
  );
end;
$function$;

create or replace function public.weekly_source_invoice_edit_context_v1(
  p_request jsonb
) returns jsonb
language plpgsql
stable
security definer
set search_path to 'public','private','pg_catalog','pg_temp'
as $function$
declare
  v_actor uuid;
  v_invoice_id uuid;
  v_invoice public.invoices%rowtype;
  v_manifest public.weekly_source_client_manifests%rowtype;
  v_result jsonb;
begin
  perform private.weekly_source_query_require_service_v1();
  if p_request is null or pg_catalog.jsonb_typeof(p_request)<>'object'
     or exists(select 1 from pg_catalog.jsonb_object_keys(p_request) key
       where key not in ('actor_user_id','invoice_id')) then
    raise exception 'WEEKLY_SOURCE_INVOICE_EDIT_CONTEXT_INVALID' using errcode='22023';
  end if;
  begin
    v_actor:=(p_request->>'actor_user_id')::uuid;
    v_invoice_id:=(p_request->>'invoice_id')::uuid;
  exception when others then
    raise exception 'WEEKLY_SOURCE_INVOICE_EDIT_CONTEXT_INVALID' using errcode='22023';
  end;
  select * into v_invoice from public.invoices where id=v_invoice_id;
  if not found or v_invoice.header_snapshot_json->>'schema_version'<>'WEEKLY_SOURCE_SELF_BILL_INVOICE_V1' then
    return pg_catalog.jsonb_build_object('is_weekly_source_invoice',false);
  end if;
  if not pg_catalog.pg_input_is_valid(
    coalesce(v_invoice.header_snapshot_json#>>'{meta,client_manifest_id}',''),'uuid'
  ) then
    raise exception 'WEEKLY_SOURCE_INVOICE_EDIT_CONTEXT_INVALID' using errcode='22023';
  end if;
  select * into strict v_manifest from public.weekly_source_client_manifests
  where id=(v_invoice.header_snapshot_json#>>'{meta,client_manifest_id}')::uuid;
  perform private.weekly_source_office_authority_v1(
    v_actor,'VIEW_SOURCE_PROGRESS',v_manifest.source_group_id,
    v_manifest.client_id,v_manifest.finalisation_week_ending
  );

  select pg_catalog.jsonb_build_object(
    'is_weekly_source_invoice',true,
    'invoice_id',v_invoice.id,'invoice_number',v_invoice.invoice_no,
    'document_revision',v_invoice.document_revision,
    'source_cycle_id',v_manifest.source_cycle_id,
    'finalisation_week_ending',v_manifest.finalisation_week_ending,
    'report_number',v_manifest.backing_report_number,
    'report_numbers',coalesce(
      v_invoice.header_snapshot_json#>'{meta,backing_report_numbers}',
      case when v_manifest.backing_report_number is null then '[]'::jsonb
        else pg_catalog.jsonb_build_array(v_manifest.backing_report_number) end
    ),
    'editable',v_invoice.status='DRAFT'
      and v_invoice.issued_at_utc is null and v_invoice.paid_at_utc is null
      and v_invoice.active_document_operation_id is null
      and v_invoice.active_issue_operation_id is null
      and pg_catalog.upper(coalesce(v_invoice.issue_state,'')) not in (
        'VALIDATING','PREPARING_DOCUMENT','READY_TO_FINALISE'
      ),
    -- Gate 7 item G7-2 (24 section 12; 25 section 8 Removed).
    -- The unit of movement offered to the Office is ONE immutable source
    -- presentation line, never a work event: "Moving by whole work-event ID is
    -- prohibited because it can move more than the Office selected."  Each row
    -- carries the presentation-line id and the expected presentation hash the
    -- move owner demands, plus the facts the Office needs to understand what
    -- will travel with it: an NHSP physical row is independently movable, a
    -- non-NHSP net presentation is indivisible, and a source-fixed expense
    -- follows its declared companion rather than being selected on its own.
    'movable_lines',coalesce((
      select pg_catalog.jsonb_agg(movable.row_json order by movable.work_date,
        movable.start_at_local,movable.candidate_display,movable.presentation_line_id)
      from (
        select presentation.id presentation_line_id,presentation.work_date,
          presentation.start_at_local,
          presentation.candidate_display_snapshot candidate_display,
          pg_catalog.jsonb_build_object(
            'presentation_line_id',presentation.id,
            'presentation_hash',pg_catalog.encode(presentation.presentation_hash,'hex'),
            'invoice_line_id',pg_catalog.min(binding.invoice_line_id::text)::uuid,
            'line_kind',presentation.line_kind,
            'origin_kind',presentation.origin_kind,
            'correction_role',presentation.correction_role,
            'work_event_id',presentation.work_event_id,
            'candidate',presentation.candidate_display_snapshot,
            'work_date',presentation.work_date,
            'start_at_local',presentation.start_at_local,
            'end_at_local',presentation.end_at_local,
            'break_minutes',presentation.break_minutes,
            'description',presentation.description_snapshot,
            'bound_movement_count',pg_catalog.count(*)::integer,
            -- A presentation that binds more than one movement is the narrowly
            -- supported non-NHSP net presentation: it moves as one.
            'indivisible_presentation',pg_catalog.count(*)>1,
            'independently_movable',
              presentation.companion_presentation_line_id is null,
            'follows_companion_presentation_line_id',
              presentation.companion_presentation_line_id,
            'companion_presentation_line_ids',coalesce((
              select pg_catalog.jsonb_agg(companion.id order by companion.id)
              from public.weekly_source_invoice_presentation_lines companion
              where companion.companion_presentation_line_id=presentation.id
            ),'[]'::jsonb),
            'released_after_dispute',exists(
              select 1 from public.weekly_discrepancy_incidents incident
              where incident.source_group_id=v_manifest.source_group_id
                and incident.work_event_id=presentation.work_event_id
            )
          ) row_json
        from public.weekly_source_invoice_line_bindings binding
        join public.weekly_source_invoice_presentation_lines presentation
          on presentation.id=binding.presentation_line_id
        where binding.invoice_id=v_invoice.id and binding.state='CURRENT'
        group by presentation.id,presentation.presentation_hash,presentation.line_kind,
          presentation.origin_kind,presentation.correction_role,presentation.work_event_id,
          presentation.candidate_display_snapshot,presentation.work_date,
          presentation.start_at_local,presentation.end_at_local,
          presentation.break_minutes,presentation.description_snapshot,
          presentation.companion_presentation_line_id
      ) movable
    ),'[]'::jsonb),
    'compatible_destinations',coalesce((
      select pg_catalog.jsonb_agg(pg_catalog.jsonb_build_object(
        'invoice_id',destination.id,'invoice_number',destination.invoice_no,
        'document_revision',destination.document_revision,
        'finalisation_week_ending',destination_manifest.finalisation_week_ending,
        'report_numbers',coalesce(destination.header_snapshot_json#>'{meta,backing_report_numbers}','[]'::jsonb),
        'total_inc_vat',destination.total_inc_vat
      ) order by destination_manifest.finalisation_week_ending desc,destination.invoice_no,destination.id)
      from public.invoices destination
      join public.weekly_source_client_manifests destination_manifest
        on destination_manifest.id=case
          when pg_catalog.pg_input_is_valid(
            coalesce(destination.header_snapshot_json#>>'{meta,client_manifest_id}',''),'uuid'
          ) then (destination.header_snapshot_json#>>'{meta,client_manifest_id}')::uuid
          else null
        end
      where destination.id<>v_invoice.id
        and destination.header_snapshot_json->>'schema_version'='WEEKLY_SOURCE_SELF_BILL_INVOICE_V1'
        and destination.client_id=v_invoice.client_id
        and destination.status='DRAFT'
        and destination.issued_at_utc is null and destination.paid_at_utc is null
        and destination.active_document_operation_id is null
        and destination.active_issue_operation_id is null
        and pg_catalog.upper(coalesce(destination.issue_state,'')) not in (
          'VALIDATING','PREPARING_DOCUMENT','READY_TO_FINALISE'
        )
    ),'[]'::jsonb)
  ) into v_result;
  return v_result;
end;
$function$;

alter function private.weekly_source_invoice_batch_snapshot_v1() owner to postgres;
alter function private.weekly_source_invoice_batch_rows_v1(jsonb,jsonb) owner to postgres;
alter function public.weekly_source_invoice_batch_candidates_v1(jsonb) owner to postgres;
alter function public.weekly_source_invoice_batch_admit_atomic_v1(jsonb) owner to postgres;
alter function public.weekly_source_invoice_edit_context_v1(jsonb) owner to postgres;

revoke all on function private.weekly_source_invoice_batch_snapshot_v1() from public,anon,authenticated;
revoke all on function private.weekly_source_invoice_batch_rows_v1(jsonb,jsonb) from public,anon,authenticated;
revoke all on function public.weekly_source_invoice_batch_candidates_v1(jsonb) from public,anon,authenticated;
revoke all on function public.weekly_source_invoice_batch_admit_atomic_v1(jsonb) from public,anon,authenticated;
revoke all on function public.weekly_source_invoice_edit_context_v1(jsonb) from public,anon,authenticated;
grant execute on function private.weekly_source_invoice_batch_snapshot_v1() to service_role;
grant execute on function private.weekly_source_invoice_batch_rows_v1(jsonb,jsonb) to service_role;
grant execute on function public.weekly_source_invoice_batch_candidates_v1(jsonb) to service_role;
grant execute on function public.weekly_source_invoice_batch_admit_atomic_v1(jsonb) to service_role;
grant execute on function public.weekly_source_invoice_edit_context_v1(jsonb) to service_role;

comment on function public.weekly_source_invoice_batch_admit_atomic_v1(jsonb) is
  'Atomically and idempotently admits an exact server-resolved set of final-source Client manifests. Each manifest remains one Client plus one finalisation cycle/report invoice; ordinary invoice generation and all pay systems are outside this function.';

commit;
