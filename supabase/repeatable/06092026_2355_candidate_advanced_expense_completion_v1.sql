-- Advanced Expense and Refusal Policy completion.
--
-- This closure keeps the first policy repeatable reviewable while replacing
-- its provisional queue/deletion projections with the final fail-closed
-- authorities. It never changes Banking Pay calculations or payment state.

\set ON_ERROR_STOP on

begin;

-- The Candidate expense-only carrier deletion is performed by the dedicated
-- Candidate system actor. That account is intentionally inactive (it cannot
-- sign in), so the established manual-adjustment preview must recognise only
-- that exact configured technical identity in addition to active Office
-- users. Keep this closure after the historical delete repeatable: editing the
-- historical file would also recreate its obsolete, unguarded two-argument
-- apply overload with PostgreSQL's default PUBLIC EXECUTE privilege.
do $candidate_delete_preview_closure$
declare
  v_definition text;
  v_old text:=E'where actor.id = p_actor_user_id\n      and actor.is_active = true';
  v_new text:=E'where actor.id = p_actor_user_id\n      and (\n        actor.is_active = true\n        or actor.id = (\n          select defaults.candidate_app_system_actor_user_id\n          from public.settings_defaults as defaults\n          where defaults.id = 1\n        )\n      )';
  v_offset integer;
begin
  select pg_catalog.pg_get_functiondef(
    'public.timesheet_weekly_manual_adjustment_delete_preview(uuid,uuid)'::regprocedure
  ) into v_definition;
  -- Historical TEST installs retained CRLF inside this function body while a
  -- clean PostgreSQL install stores LF. Normalise only the retrieved DDL text
  -- before the exact single-occurrence closure check so both represent the
  -- same reviewed function without weakening the drift guard.
  v_definition:=pg_catalog.replace(v_definition,E'\r\n',E'\n');
  if pg_catalog.strpos(v_definition,v_new)>0 then
    return;
  end if;
  v_offset:=pg_catalog.strpos(v_definition,v_old);
  if v_offset=0
     or pg_catalog.strpos(
       pg_catalog.substr(v_definition,v_offset+pg_catalog.length(v_old)),v_old
     )>0 then
    raise exception 'CANDIDATE_EXPENSE_DELETE_PREVIEW_CLOSURE_DRIFT'
      using errcode='55000';
  end if;
  execute pg_catalog.replace(v_definition,v_old,v_new);
end;
$candidate_delete_preview_closure$;

drop function if exists public.timesheet_weekly_manual_adjustment_delete_apply(uuid,uuid);
-- Remove the provisional pre-policy whole-claim overload. The locked policy
-- requires the exact claim-scope digest and no caller may bypass that check by
-- resolving an older signature retained by an incremental database.
drop function if exists public.candidate_whole_claim_action_atomic_v1(
  uuid,text,uuid,integer,uuid,text,text,text,timestamptz
);
alter function public.timesheet_weekly_manual_adjustment_delete_preview(uuid,uuid) owner to postgres;
revoke all on function public.timesheet_weekly_manual_adjustment_delete_preview(uuid,uuid)
  from public,anon,authenticated,service_role;
grant execute on function public.timesheet_weekly_manual_adjustment_delete_preview(uuid,uuid)
  to service_role;
alter function public.timesheet_weekly_manual_adjustment_delete_apply(
  uuid,uuid,uuid[],uuid[],uuid[],uuid[],text
) owner to postgres;
revoke all on function public.timesheet_weekly_manual_adjustment_delete_apply(
  uuid,uuid,uuid[],uuid[],uuid[],uuid[],text
) from public,anon,authenticated,service_role;
grant execute on function public.timesheet_weekly_manual_adjustment_delete_apply(
  uuid,uuid,uuid[],uuid[],uuid[],uuid[],text
) to service_role;

-- An expense summary render is immutable. A newer financial total supersedes
-- an in-flight render instead of rewriting its input beneath the renderer.
create or replace function private._candidate_expense_summary_queue_v1(
  p_timesheet_id uuid,
  p_now_utc timestamptz default now()
)
returns jsonb
language plpgsql
volatile
security definer
set search_path = pg_catalog, public, private, extensions, pg_temp
as $function$
declare
  v_fin public.timesheets_financials%rowtype;
  v_total numeric;
  v_has_summary boolean;
  v_totals jsonb;
  v_digest bytea;
  v_generation integer;
  v_refresh public.candidate_expense_summary_refreshes%rowtype;
  v_identity jsonb;
  v_evidence_counts jsonb;
  v_categories jsonb;
  v_cleanup_failures jsonb:='[]'::jsonb;
  v_cleanup_record jsonb;
begin
  if p_timesheet_id is null then
    raise exception 'TIMESHEET_ID_REQUIRED' using errcode='22023';
  end if;
  perform pg_catalog.pg_advisory_xact_lock(
    pg_catalog.hashtextextended('candidate-expense-summary|'||p_timesheet_id::text,0)
  );
  select financial.* into v_fin
  from public.timesheets_financials financial
  where financial.timesheet_id=p_timesheet_id and financial.is_current
  order by financial.computed_at_utc desc nulls last,
    financial.updated_at desc,financial.id desc
  limit 1 for update;
  if not found then
    return jsonb_build_object('ok',true,'timesheet_id',p_timesheet_id,
      'summary_state','NOT_REQUIRED');
  end if;
  v_total:=coalesce(v_fin.mileage_pay_ex_vat,0)
    +coalesce(v_fin.travel_pay_ex_vat,0)
    +coalesce(v_fin.accommodation_pay_ex_vat,0)
    +coalesce(v_fin.other_pay_ex_vat,0);
  select jsonb_build_object(
    'candidate_name',nullif(btrim(coalesce(candidate.display_name,
      concat_ws(' ',candidate.first_name,candidate.last_name))),''),
    'candidate_reference',candidate.tms_ref,
    'client_name',client.name,'client_reference',client.cli_ref,
    'contract_id',contract.id,'contract_role',contract.role,
    'contract_site',contract.display_site,
    'week_ending_date',v_timesheet.week_ending_date
  ) into v_identity
  from public.timesheets v_timesheet
  left join public.contracts contract on contract.id=v_timesheet.contract_id
  left join public.candidates candidate on candidate.id=v_fin.candidate_id
  left join public.clients client on client.id=v_fin.client_id
  where v_timesheet.timesheet_id=p_timesheet_id;
  select coalesce(jsonb_object_agg(category.expense_category,
    category.source_count+category.materialised_only_count
    order by category.expense_category),'{}'::jsonb)
  into v_evidence_counts
  from (
    select requested.expense_category,
      (select count(distinct coalesce(source.source_component_id,source.id))::integer
       from public.candidate_expense_components component
       join public.candidate_submission_components source
         on source.workflow_id=component.workflow_id
        and source.workflow_generation=component.workflow_generation
        and source.expense_category=component.expense_category
        and source.component_kind in ('MILEAGE_FORM','EXPENSE_EVIDENCE')
        and source.state not in ('SUPERSEDED','REJECTED','ABANDONED')
       where component.expense_category=requested.expense_category
         and component.lifecycle_state not in (
           'MANAGER_REFUSED','OFFICE_REJECTED','WITHDRAWN','CANCELLED','SUPERSEDED'
         )
         and private._candidate_expense_owned_timesheet_id_v1(
           component.workflow_id,component.owning_timesheet_id
         )=p_timesheet_id) as source_count,
      (select count(distinct evidence.id)::integer
       from public.timesheet_evidence evidence
       where evidence.timesheet_id=p_timesheet_id
         and evidence.processing_state<>'SUPERSEDED'
         and evidence.document_role<>'EXPENSE_MILEAGE_APPROVAL_SUMMARY'
         and upper(coalesce(evidence.kind::text,''))=requested.expense_category
         and (evidence.candidate_component_id is null or not exists(
           select 1
           from public.candidate_expense_components active_component
           join public.candidate_submission_components source
             on source.workflow_id=active_component.workflow_id
            and source.workflow_generation=active_component.workflow_generation
            and source.expense_category=active_component.expense_category
            and source.component_kind in ('MILEAGE_FORM','EXPENSE_EVIDENCE')
            and source.state not in ('SUPERSEDED','REJECTED','ABANDONED')
           left join public.candidate_submission_components materialised_source
             on materialised_source.id=evidence.candidate_component_id
           where active_component.expense_category=requested.expense_category
             and active_component.lifecycle_state not in (
               'MANAGER_REFUSED','OFFICE_REJECTED','WITHDRAWN','CANCELLED','SUPERSEDED'
             )
             and private._candidate_expense_owned_timesheet_id_v1(
               active_component.workflow_id,active_component.owning_timesheet_id
             )=p_timesheet_id
             and coalesce(source.source_component_id,source.id)
               =coalesce(materialised_source.source_component_id,
                 evidence.candidate_component_id)
         ))) as materialised_only_count
    from (values ('MILEAGE'),('TRAVEL'),('ACCOMMODATION'),('OTHER'))
      requested(expense_category)
  ) category;
  select coalesce(jsonb_agg(jsonb_build_object(
    'expense_category',category.expense_category,
    'amount',category.amount,
    'mileage_units',category.mileage_units,
    'supporting_evidence_count',coalesce(
      (v_evidence_counts->>category.expense_category)::integer,0
    )
  ) order by category.ordinal),'[]'::jsonb)
  into v_categories
  from (
    values
      (1,'MILEAGE',coalesce(v_fin.mileage_pay_ex_vat,0),coalesce(v_fin.mileage_units,0)),
      (2,'TRAVEL',coalesce(v_fin.travel_pay_ex_vat,0),0::numeric),
      (3,'ACCOMMODATION',coalesce(v_fin.accommodation_pay_ex_vat,0),0::numeric),
      (4,'OTHER',coalesce(v_fin.other_pay_ex_vat,0),0::numeric)
  ) category(ordinal,expense_category,amount,mileage_units)
  where category.amount<>0 or category.mileage_units<>0;
  v_has_summary:=jsonb_array_length(v_categories)>0;
  v_totals:=jsonb_build_object(
    'identity',coalesce(v_identity,'{}'::jsonb),
    'evidence_counts',coalesce(v_evidence_counts,'{}'::jsonb),
    'categories',v_categories,
    'mileage_units',coalesce(v_fin.mileage_units,0),
    'mileage_pay_ex_vat',coalesce(v_fin.mileage_pay_ex_vat,0),
    'travel_pay_ex_vat',coalesce(v_fin.travel_pay_ex_vat,0),
    'accommodation_pay_ex_vat',coalesce(v_fin.accommodation_pay_ex_vat,0),
    'other_pay_ex_vat',coalesce(v_fin.other_pay_ex_vat,0),
    'expenses_pay_ex_vat',coalesce(v_fin.travel_pay_ex_vat,0)
      +coalesce(v_fin.accommodation_pay_ex_vat,0)
      +coalesce(v_fin.other_pay_ex_vat,0),
    'total_pay_ex_vat',v_total
  );
  v_digest:=extensions.digest(pg_catalog.convert_to(v_totals::text,'UTF8'),'sha256');

  select refresh.* into v_refresh
  from public.candidate_expense_summary_refreshes refresh
  where refresh.timesheet_id=p_timesheet_id
    and refresh.state in ('PENDING','RENDERING','READY','REMOVED')
  order by refresh.summary_generation desc
  limit 1 for update;
  if found and v_refresh.totals_sha256=v_digest
     and ((not v_has_summary and v_refresh.state='REMOVED')
       or (v_has_summary and v_refresh.state in ('PENDING','RENDERING','READY'))) then
    return jsonb_build_object(
      'ok',true,'timesheet_id',p_timesheet_id,
      'refresh_id',v_refresh.refresh_id,
      'summary_generation',v_refresh.summary_generation,
      'summary_state',v_refresh.state,'totals',v_totals,
      'idempotent_replay',true
    );
  end if;

  update public.candidate_expense_summary_refreshes refresh set
    state='SUPERSEDED',completed_at_utc=p_now_utc,updated_at_utc=p_now_utc,
    -- Retain the exact in-flight claim identity on a superseded render.  Its
    -- late completion can then prove and durably retire the attempt-specific
    -- R2 object instead of orphaning it.  A merely PENDING row has no claim.
    claim_token=case when refresh.state='RENDERING' then refresh.claim_token end,
    claimed_at_utc=case when refresh.state='RENDERING' then refresh.claimed_at_utc end,
    lease_expires_at_utc=case when refresh.state='RENDERING'
      then refresh.lease_expires_at_utc end
  where refresh.timesheet_id=p_timesheet_id
    and refresh.state in ('PENDING','RENDERING');

  select coalesce(max(refresh.summary_generation),0)+1 into v_generation
  from public.candidate_expense_summary_refreshes refresh
  where refresh.timesheet_id=p_timesheet_id;
  insert into public.candidate_expense_summary_refreshes(
    timesheet_id,timesheet_id_snapshot,summary_generation,state,totals_json,totals_sha256,
    source_financials_id,idempotency_key,requested_at_utc,completed_at_utc,
    updated_at_utc
  ) values (
    p_timesheet_id,p_timesheet_id,v_generation,
    case when not v_has_summary then 'REMOVED' else 'PENDING' end,
    v_totals,v_digest,v_fin.id,
    'expense-summary:'||p_timesheet_id::text||':'||v_generation::text||':'||encode(v_digest,'hex'),
    p_now_utc,case when not v_has_summary then p_now_utc else null end,p_now_utc
  ) returning * into v_refresh;

  if not v_has_summary then
    select coalesce(jsonb_agg(jsonb_build_object(
      'r2_key',current_summary.storage_key,'attempt_count',1,
      'error','EXPENSE_SUMMARY_REMOVED'
    ) order by current_summary.storage_key),'[]'::jsonb)
    into v_cleanup_failures
    from (
      select distinct evidence.storage_key
      from public.timesheet_evidence evidence
      where evidence.timesheet_id=p_timesheet_id
        and evidence.document_role='EXPENSE_MILEAGE_APPROVAL_SUMMARY'
        and evidence.processing_state<>'SUPERSEDED'
        and nullif(btrim(coalesce(evidence.storage_key,'')),'') is not null
    ) current_summary;
    update public.timesheet_evidence evidence set processing_state='SUPERSEDED'
    where evidence.timesheet_id=p_timesheet_id
      and evidence.document_role='EXPENSE_MILEAGE_APPROVAL_SUMMARY'
      and evidence.processing_state<>'SUPERSEDED';
    if jsonb_array_length(v_cleanup_failures)>0 then
      v_cleanup_record:=public.timesheet_r2_cleanup_record_v1(
        'candidate-expense-summary-remove:'||v_refresh.refresh_id::text,
        p_timesheet_id,array[]::uuid[],v_cleanup_failures,null
      );
      if coalesce((v_cleanup_record->>'ok')::boolean,false) is not true
         or coalesce((v_cleanup_record->>'valid_distinct_keys')::integer,-1)
            is distinct from jsonb_array_length(v_cleanup_failures) then
        raise exception 'CANDIDATE_EXPENSE_SUMMARY_CLEANUP_NOT_DURABLE'
          using errcode='40001';
      end if;
    end if;
  end if;
  return jsonb_build_object(
    'ok',true,'timesheet_id',p_timesheet_id,
    'refresh_id',v_refresh.refresh_id,
    'summary_generation',v_refresh.summary_generation,
    'summary_state',v_refresh.state,'totals',v_totals,
    'idempotent_replay',false
  );
end;
$function$;

create or replace function public.candidate_expense_summary_claim_v1(
  p_limit integer default 10,
  p_lease_seconds integer default 300,
  p_now_utc timestamptz default now()
)
returns jsonb
language plpgsql
volatile
security definer
set search_path = pg_catalog, public, private, pg_temp
as $function$
declare
  v_limit integer:=greatest(1,least(coalesce(p_limit,10),25));
  v_lease integer:=greatest(30,least(coalesce(p_lease_seconds,300),900));
  v_claim_token uuid:=pg_catalog.gen_random_uuid();
  v_jobs jsonb;
  v_expired record;
  v_cleanup jsonb;
begin
  -- A renderer registers its deterministic attempt key before writing R2.
  -- Reclaiming an expired lease first records that key in the durable cleanup
  -- queue, closing the process-crash window between R2 PUT and completion.
  for v_expired in
    select refresh.refresh_id,refresh.claim_token,
      refresh.attempt_storage_key,
      coalesce(refresh.timesheet_id,refresh.timesheet_id_snapshot) as timesheet_id
    from public.candidate_expense_summary_refreshes refresh
    where refresh.state in ('RENDERING','SUPERSEDED')
      and refresh.lease_expires_at_utc<=p_now_utc
      and refresh.attempt_storage_key is not null
    order by refresh.refresh_id
    for update skip locked
  loop
    v_cleanup:=public.timesheet_r2_cleanup_record_v1(
      'candidate-expense-summary-expired:'||v_expired.refresh_id::text||':'
        ||v_expired.claim_token::text,
      v_expired.timesheet_id,array[]::uuid[],jsonb_build_array(jsonb_build_object(
        'r2_key',v_expired.attempt_storage_key,'attempt_count',1,
        'error','CANDIDATE_EXPENSE_SUMMARY_LEASE_EXPIRED'
      )),null
    );
    if coalesce((v_cleanup->>'recorded')::integer,0)=0
       and coalesce((v_cleanup->>'stale_or_complete_ignored')::integer,0)=0 then
      raise exception 'CANDIDATE_EXPENSE_SUMMARY_CLEANUP_NOT_DURABLE'
        using errcode='40001';
    end if;
    update public.candidate_expense_summary_refreshes refresh set
      attempt_storage_key=null,updated_at_utc=p_now_utc
    where refresh.refresh_id=v_expired.refresh_id
      and refresh.claim_token=v_expired.claim_token;
  end loop;
  update public.candidate_expense_summary_refreshes refresh set
    state='FAILED',failure_code='CANDIDATE_EXPENSE_SUMMARY_LEASE_EXHAUSTED',
    claim_token=null,claimed_at_utc=null,lease_expires_at_utc=null,
    completed_at_utc=p_now_utc,updated_at_utc=p_now_utc
  where refresh.state='RENDERING'
    and refresh.lease_expires_at_utc<=p_now_utc
    and refresh.attempt_count>=5;
  with candidates as (
    select refresh.refresh_id
    from public.candidate_expense_summary_refreshes refresh
    where (
      refresh.state='PENDING'
      or (refresh.state='RENDERING' and refresh.lease_expires_at_utc<=p_now_utc)
    ) and refresh.attempt_count<5
    order by refresh.requested_at_utc,refresh.refresh_id
    limit v_limit
    for update skip locked
  ), claimed as (
    update public.candidate_expense_summary_refreshes refresh set
      state='RENDERING',claim_token=v_claim_token,claimed_at_utc=p_now_utc,
      attempt_storage_key=null,
      lease_expires_at_utc=p_now_utc+pg_catalog.make_interval(secs=>v_lease),
      attempt_count=refresh.attempt_count+1,failure_code=null,
      completed_at_utc=null,updated_at_utc=p_now_utc
    from candidates
    where refresh.refresh_id=candidates.refresh_id
    returning refresh.*
  )
  select coalesce(jsonb_agg(jsonb_build_object(
    'refresh_id',claimed.refresh_id,
    'claim_token',claimed.claim_token,
    'timesheet_id',coalesce(claimed.timesheet_id,claimed.timesheet_id_snapshot),
    'summary_generation',claimed.summary_generation,
    'totals',claimed.totals_json,
    'totals_sha256',encode(claimed.totals_sha256,'hex'),
    'attempt_count',claimed.attempt_count,
    'lease_expires_at_utc',claimed.lease_expires_at_utc
  ) order by claimed.requested_at_utc,claimed.refresh_id),'[]'::jsonb)
  into v_jobs from claimed;
  return jsonb_build_object('ok',true,'claim_token',v_claim_token,
    'claimed_count',jsonb_array_length(v_jobs),'jobs',v_jobs);
end;
$function$;

create or replace function public.candidate_expense_summary_render_begin_v1(
  p_refresh_id uuid,
  p_claim_token uuid,
  p_expected_totals_sha256 text,
  p_storage_key text,
  p_now_utc timestamptz default now()
)
returns jsonb
language plpgsql
volatile
security definer
set search_path = pg_catalog, public, private, pg_temp
as $function$
declare
  v_refresh public.candidate_expense_summary_refreshes%rowtype;
  v_suffix text;
begin
  if p_refresh_id is null or p_claim_token is null
     or coalesce(p_expected_totals_sha256,'') !~ '^[0-9a-f]{64}$'
     or nullif(btrim(coalesce(p_storage_key,'')),'') is null then
    raise exception 'CANDIDATE_EXPENSE_SUMMARY_RECEIPT_INVALID' using errcode='22023';
  end if;
  select refresh.* into v_refresh
  from public.candidate_expense_summary_refreshes refresh
  where refresh.refresh_id=p_refresh_id for update;
  if not found then
    raise exception 'CANDIDATE_EXPENSE_SUMMARY_NOT_FOUND' using errcode='P0002';
  end if;
  v_suffix:='/expense-summaries/'
    ||coalesce(v_refresh.timesheet_id,v_refresh.timesheet_id_snapshot)::text||'/'
    ||lpad(v_refresh.summary_generation::text,6,'0')||'-'
    ||lower(p_expected_totals_sha256)||'-'||p_claim_token::text||'.pdf';
  if coalesce(v_refresh.state,'')<>'RENDERING'
     or v_refresh.claim_token is distinct from p_claim_token
     or v_refresh.lease_expires_at_utc is null
     or v_refresh.lease_expires_at_utc<=p_now_utc
     or v_refresh.totals_sha256 is distinct from decode(lower(p_expected_totals_sha256),'hex')
     or right(p_storage_key,char_length(v_suffix)) is distinct from v_suffix then
    raise exception 'CANDIDATE_EXPENSE_SUMMARY_STALE' using errcode='40001';
  end if;
  if v_refresh.attempt_storage_key is not null
     and v_refresh.attempt_storage_key is distinct from p_storage_key then
    raise exception 'CANDIDATE_EXPENSE_SUMMARY_RECEIPT_INVALID' using errcode='40001';
  end if;
  update public.candidate_expense_summary_refreshes refresh set
    attempt_storage_key=p_storage_key,updated_at_utc=p_now_utc
  where refresh.refresh_id=v_refresh.refresh_id;
  return jsonb_build_object('ok',true,'refresh_id',v_refresh.refresh_id,
    'claim_token',p_claim_token,'storage_key',p_storage_key,
    'idempotent_replay',v_refresh.attempt_storage_key is not distinct from p_storage_key);
end;
$function$;

create or replace function public.candidate_expense_summary_complete_v1(
  p_refresh_id uuid,
  p_claim_token uuid,
  p_expected_totals_sha256 text,
  p_storage_key text,
  p_summary_sha256 text,
  p_now_utc timestamptz default now()
)
returns jsonb
language plpgsql
volatile
security definer
set search_path = pg_catalog, public, private, pg_temp
as $function$
declare
  v_refresh public.candidate_expense_summary_refreshes%rowtype;
  v_evidence_id uuid;
  v_storage_suffix text;
  v_cleanup jsonb;
  v_prior_cleanup_failures jsonb:='[]'::jsonb;
  v_prior_cleanup jsonb;
begin
  if p_refresh_id is null or p_claim_token is null
     or coalesce(p_expected_totals_sha256,'') !~ '^[0-9a-f]{64}$'
     or coalesce(p_summary_sha256,'') !~ '^[0-9a-f]{64}$'
     or nullif(pg_catalog.btrim(coalesce(p_storage_key,'')),'') is null then
    raise exception 'CANDIDATE_EXPENSE_SUMMARY_RECEIPT_INVALID' using errcode='22023';
  end if;
  select refresh.* into v_refresh
  from public.candidate_expense_summary_refreshes refresh
  where refresh.refresh_id=p_refresh_id for update;
  if not found then
    raise exception 'CANDIDATE_EXPENSE_SUMMARY_NOT_FOUND' using errcode='P0002';
  end if;
  -- Each render attempt owns a different immutable object.  This makes a
  -- late/expired attempt safe to clean without racing the winning claim.
  v_storage_suffix:='/expense-summaries/'
    ||coalesce(v_refresh.timesheet_id,v_refresh.timesheet_id_snapshot)::text||'/'
    ||lpad(v_refresh.summary_generation::text,6,'0')||'-'
    ||lower(p_expected_totals_sha256)||'-'||p_claim_token::text||'.pdf';
  if right(p_storage_key,char_length(v_storage_suffix)) is distinct from v_storage_suffix then
    raise exception 'CANDIDATE_EXPENSE_SUMMARY_RECEIPT_INVALID' using errcode='22023';
  end if;
  if v_refresh.state='READY'
     and v_refresh.totals_sha256 is not distinct from decode(p_expected_totals_sha256,'hex')
     and v_refresh.summary_storage_key is not distinct from p_storage_key
     and v_refresh.summary_sha256 is not distinct from decode(p_summary_sha256,'hex') then
    return jsonb_build_object('ok',true,'refresh_id',v_refresh.refresh_id,
      'timesheet_id',coalesce(v_refresh.timesheet_id,v_refresh.timesheet_id_snapshot),
      'summary_generation',v_refresh.summary_generation,
      'state','READY','idempotent_replay',true);
  end if;
  if v_refresh.state='RENDERING' and v_refresh.claim_token is not distinct from p_claim_token
     and v_refresh.attempt_storage_key is distinct from p_storage_key then
    raise exception 'CANDIDATE_EXPENSE_SUMMARY_RECEIPT_INVALID' using errcode='40001';
  end if;
  if coalesce(v_refresh.state,'')<>'RENDERING'
     or v_refresh.claim_token is distinct from p_claim_token
     or v_refresh.lease_expires_at_utc is null
     or v_refresh.lease_expires_at_utc<=p_now_utc
     or v_refresh.totals_sha256 is distinct from decode(p_expected_totals_sha256,'hex')
     or exists(
       select 1 from public.candidate_expense_summary_refreshes newer
       where newer.timesheet_id=v_refresh.timesheet_id
         and newer.summary_generation>v_refresh.summary_generation
     ) then
    -- The attempt-specific object may already have been written before the
    -- database discovered that this render lost the race.  Record exact
    -- cleanup durably and return a truthful stale receipt; never throw after
    -- the storage side effect and lose the only cleanup authority.
    v_cleanup:=public.timesheet_r2_cleanup_record_v1(
      'candidate-expense-summary-stale:'||v_refresh.refresh_id::text||':'
        ||p_claim_token::text,
      coalesce(v_refresh.timesheet_id,v_refresh.timesheet_id_snapshot),
      array[]::uuid[],jsonb_build_array(jsonb_build_object(
        'r2_key',p_storage_key,'attempt_count',1,
        'error','CANDIDATE_EXPENSE_SUMMARY_STALE'
      )),null
    );
    if v_refresh.state='RENDERING' and v_refresh.claim_token is not distinct from p_claim_token then
      update public.candidate_expense_summary_refreshes refresh set
        state='SUPERSEDED',failure_code='CANDIDATE_EXPENSE_SUMMARY_STALE',
        attempt_storage_key=null,
        claim_token=null,claimed_at_utc=null,lease_expires_at_utc=null,
        completed_at_utc=p_now_utc,updated_at_utc=p_now_utc
      where refresh.refresh_id=v_refresh.refresh_id;
    elsif v_refresh.state='SUPERSEDED' and v_refresh.claim_token is not distinct from p_claim_token then
      update public.candidate_expense_summary_refreshes refresh set
        attempt_storage_key=null,claim_token=null,claimed_at_utc=null,lease_expires_at_utc=null,
        updated_at_utc=p_now_utc
      where refresh.refresh_id=v_refresh.refresh_id;
    end if;
    return jsonb_build_object(
      'ok',true,'refresh_id',v_refresh.refresh_id,
      'timesheet_id',coalesce(v_refresh.timesheet_id,v_refresh.timesheet_id_snapshot),
      'summary_generation',v_refresh.summary_generation,
      'state','SUPERSEDED','stale',true,
      'r2_cleanup_durable',coalesce((v_cleanup->>'recorded')::integer,0)>0
        or coalesce((v_cleanup->>'stale_or_complete_ignored')::integer,0)>0,
      'idempotent_replay',false
    );
  end if;
  if v_refresh.timesheet_id is null then
    raise exception 'CANDIDATE_EXPENSE_SUMMARY_STALE' using errcode='40001';
  end if;
  select coalesce(jsonb_agg(jsonb_build_object(
    'r2_key',current_summary.storage_key,'attempt_count',1,
    'error','EXPENSE_SUMMARY_REPLACED'
  ) order by current_summary.storage_key),'[]'::jsonb)
  into v_prior_cleanup_failures
  from (
    select distinct evidence.storage_key
    from public.timesheet_evidence evidence
    where evidence.timesheet_id=v_refresh.timesheet_id
      and evidence.document_role='EXPENSE_MILEAGE_APPROVAL_SUMMARY'
      and evidence.processing_state<>'SUPERSEDED'
      and evidence.storage_key<>p_storage_key
      and nullif(btrim(coalesce(evidence.storage_key,'')),'') is not null
  ) current_summary;
  update public.timesheet_evidence evidence set processing_state='SUPERSEDED'
  where evidence.timesheet_id=v_refresh.timesheet_id
    and evidence.document_role='EXPENSE_MILEAGE_APPROVAL_SUMMARY'
    and evidence.processing_state<>'SUPERSEDED';
  if jsonb_array_length(v_prior_cleanup_failures)>0 then
    v_prior_cleanup:=public.timesheet_r2_cleanup_record_v1(
      'candidate-expense-summary-replace:'||v_refresh.refresh_id::text,
      v_refresh.timesheet_id,array[]::uuid[],v_prior_cleanup_failures,null
    );
    if coalesce((v_prior_cleanup->>'ok')::boolean,false) is not true
       or coalesce((v_prior_cleanup->>'valid_distinct_keys')::integer,-1)
          is distinct from jsonb_array_length(v_prior_cleanup_failures) then
      raise exception 'CANDIDATE_EXPENSE_SUMMARY_CLEANUP_NOT_DURABLE'
        using errcode='40001';
    end if;
  end if;
  insert into public.timesheet_evidence(
    timesheet_id,kind,display_name,storage_key,created_at,
    source_revision,processing_state,document_role
  ) values (
    v_refresh.timesheet_id,'OTHER','Expense summary',p_storage_key,p_now_utc,
    'expense-summary:'||v_refresh.summary_generation::text||':'
      ||p_expected_totals_sha256,'READY','EXPENSE_MILEAGE_APPROVAL_SUMMARY'
  ) returning id into v_evidence_id;
  update public.candidate_expense_summary_refreshes refresh set
    state='READY',summary_storage_key=p_storage_key,
    summary_sha256=decode(p_summary_sha256,'hex'),completed_at_utc=p_now_utc,
    attempt_storage_key=null,lease_expires_at_utc=null,updated_at_utc=p_now_utc
  where refresh.refresh_id=v_refresh.refresh_id;
  return jsonb_build_object('ok',true,'refresh_id',v_refresh.refresh_id,
    'timesheet_id',v_refresh.timesheet_id,'summary_generation',v_refresh.summary_generation,
    'state','READY','timesheet_evidence_id',v_evidence_id,'idempotent_replay',false);
end;
$function$;

drop function if exists public.candidate_expense_summary_fail_v1(
  uuid,uuid,text,timestamptz
);
create or replace function public.candidate_expense_summary_fail_v1(
  p_refresh_id uuid,
  p_claim_token uuid,
  p_failure_code text,
  p_storage_key text,
  p_now_utc timestamptz default now()
)
returns jsonb
language plpgsql
volatile
security definer
set search_path = pg_catalog, public, private, pg_temp
as $function$
declare
  v_refresh public.candidate_expense_summary_refreshes%rowtype;
  v_code text:=upper(pg_catalog.btrim(coalesce(p_failure_code,'')));
  v_terminal boolean;
  v_storage_suffix text;
  v_cleanup jsonb;
  v_cleanup_durable boolean:=false;
begin
  if p_refresh_id is null or p_claim_token is null
     or v_code !~ '^[A-Z][A-Z0-9_]{2,99}$' then
    raise exception 'CANDIDATE_EXPENSE_SUMMARY_FAILURE_INVALID' using errcode='22023';
  end if;
  select refresh.* into v_refresh
  from public.candidate_expense_summary_refreshes refresh
  where refresh.refresh_id=p_refresh_id for update;
  if not found then
    raise exception 'CANDIDATE_EXPENSE_SUMMARY_NOT_FOUND' using errcode='P0002';
  end if;
  if nullif(btrim(coalesce(p_storage_key,'')),'') is not null then
    v_storage_suffix:='/expense-summaries/'
      ||coalesce(v_refresh.timesheet_id,v_refresh.timesheet_id_snapshot)::text||'/'
      ||lpad(v_refresh.summary_generation::text,6,'0')||'-'
      ||encode(v_refresh.totals_sha256,'hex')||'-'||p_claim_token::text||'.pdf';
    if right(p_storage_key,char_length(v_storage_suffix)) is distinct from v_storage_suffix then
      raise exception 'CANDIDATE_EXPENSE_SUMMARY_RECEIPT_INVALID' using errcode='22023';
    end if;
    if v_refresh.claim_token is not distinct from p_claim_token
       and v_refresh.attempt_storage_key is not null
       and v_refresh.attempt_storage_key is distinct from p_storage_key then
      raise exception 'CANDIDATE_EXPENSE_SUMMARY_RECEIPT_INVALID' using errcode='40001';
    end if;
    if v_refresh.state='READY'
       and v_refresh.summary_storage_key is not distinct from p_storage_key then
      return jsonb_build_object('ok',true,'refresh_id',v_refresh.refresh_id,
        'state','READY','idempotent_replay',true);
    end if;
    v_cleanup:=public.timesheet_r2_cleanup_record_v1(
      'candidate-expense-summary-failed:'||v_refresh.refresh_id::text||':'
        ||p_claim_token::text,
      coalesce(v_refresh.timesheet_id,v_refresh.timesheet_id_snapshot),
      array[]::uuid[],jsonb_build_array(jsonb_build_object(
        'r2_key',p_storage_key,'attempt_count',1,'error',v_code
      )),null
    );
    v_cleanup_durable:=coalesce((v_cleanup->>'recorded')::integer,0)>0
      or coalesce((v_cleanup->>'stale_or_complete_ignored')::integer,0)>0;
    if not v_cleanup_durable then
      raise exception 'CANDIDATE_EXPENSE_SUMMARY_CLEANUP_NOT_DURABLE'
        using errcode='40001';
    end if;
  end if;
  if v_refresh.state='SUPERSEDED'
     or (nullif(btrim(coalesce(p_storage_key,'')),'') is not null and (
       coalesce(v_refresh.state,'')<>'RENDERING'
       or v_refresh.claim_token is distinct from p_claim_token
     )) then
    return jsonb_build_object('ok',true,'refresh_id',v_refresh.refresh_id,
      'state','SUPERSEDED','stale',true,
      'r2_cleanup_durable',v_cleanup_durable,'idempotent_replay',true);
  end if;
  if v_refresh.state not in ('RENDERING','FAILED')
     or (v_refresh.state='RENDERING'
       and v_refresh.claim_token is distinct from p_claim_token) then
    raise exception 'CANDIDATE_EXPENSE_SUMMARY_STALE' using errcode='40001';
  end if;
  if v_refresh.state='FAILED' then
    return jsonb_build_object('ok',true,'refresh_id',v_refresh.refresh_id,
      'state','FAILED','r2_cleanup_durable',v_cleanup_durable,
      'idempotent_replay',true);
  end if;
  v_terminal:=v_refresh.attempt_count>=5;
  update public.candidate_expense_summary_refreshes refresh set
    state=case when v_terminal then 'FAILED' else 'PENDING' end,
    failure_code=v_code,attempt_storage_key=null,claim_token=null,claimed_at_utc=null,
    lease_expires_at_utc=null,completed_at_utc=case when v_terminal then p_now_utc else null end,
    updated_at_utc=p_now_utc
  where refresh.refresh_id=v_refresh.refresh_id;
  return jsonb_build_object('ok',true,'refresh_id',v_refresh.refresh_id,
    'state',case when v_terminal then 'FAILED' else 'PENDING' end,
    'retryable',not v_terminal,'r2_cleanup_durable',v_cleanup_durable,
    'idempotent_replay',false);
end;
$function$;

-- Delete only a proved-empty Candidate expense carrier through the existing
-- guarded weekly manual-adjustment deletion authority. An isolated legacy
-- carrier with no proved worked/base source is retained (rather than routed
-- through the broader parent-chain authority). Candidate audit rows are
-- detached and tombstoned first; any delete failure rolls the transaction
-- back, including those detachments. Banking Pay receives the normal existing
-- deletion signals and its economic rules are not bypassed or reimplemented.
create or replace function private._candidate_zero_expense_carrier_delete_v1(
  p_environment text,
  p_timesheet_id uuid,
  p_operation_id uuid,
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
  v_timesheet public.timesheets%rowtype;
  v_fin public.timesheets_financials%rowtype;
  v_system_actor uuid;
  v_preview jsonb;
  v_signature jsonb;
  v_row_signature text;
  v_timesheet_ids uuid[]:=array[]::uuid[];
  v_contract_week_ids uuid[]:=array[]::uuid[];
  v_source_timesheet_ids uuid[]:=array[]::uuid[];
  v_source_contract_week_ids uuid[]:=array[]::uuid[];
  v_source_timesheet_id uuid;
  v_source_contract_week_id uuid;
  v_result jsonb;
  v_cleanup_failures jsonb:='[]'::jsonb;
  v_cleanup_record jsonb;
  v_expected_delete_scope_sha256 text;
  v_delete_scope jsonb;
  v_delete_scope_sha256 text;
  v_empty_timesheet_consequence text:='NONE';
begin
  if p_timesheet_id is null or p_operation_id is null then
    raise exception 'CANDIDATE_EXPENSE_CARRIER_DELETE_INVALID' using errcode='22023';
  end if;
  select row.* into v_timesheet from public.timesheets row
  where row.timesheet_id=p_timesheet_id and row.is_current
    and row.archived_at_utc is null for update;
  if not found then
    if exists(
      select 1 from public.timesheets row
      where row.timesheet_id=p_timesheet_id
    ) then
      return jsonb_build_object(
        'ok',true,'owning_timesheet_deleted',false,
        'previous_owning_timesheet_id',p_timesheet_id,
        'empty_timesheet_consequence','REMOVE_FROM_CURRENT_KEEP_HISTORY',
        'deleted_timesheet_ids','[]'::jsonb,
        'retained_timesheet_ids',jsonb_build_array(p_timesheet_id),
        'affected_timesheet_ids',jsonb_build_array(p_timesheet_id),
        'removed_from_current_timesheet_ids',jsonb_build_array(p_timesheet_id),
        'r2_cleanup_keys','[]'::jsonb,'idempotent_replay',true
      );
    end if;
    return jsonb_build_object('ok',true,'owning_timesheet_deleted',true,
      'previous_owning_timesheet_id',p_timesheet_id,
      'empty_timesheet_consequence','PERMANENT_REMOVE',
      'deleted_timesheet_ids',jsonb_build_array(p_timesheet_id),
      'retained_timesheet_ids','[]'::jsonb,
      'affected_timesheet_ids',jsonb_build_array(p_timesheet_id),
      'removed_from_current_timesheet_ids',jsonb_build_array(p_timesheet_id),
      'r2_cleanup_keys','[]'::jsonb,
      'idempotent_replay',true);
  end if;
  select row.* into v_fin from public.timesheets_financials row
  where row.timesheet_id=p_timesheet_id and row.is_current
  order by row.computed_at_utc desc nulls last,row.updated_at desc,row.id desc
  limit 1 for update;
  if not found
     or coalesce(v_fin.total_hours,0)<>0
     or coalesce(v_fin.mileage_units,0)<>0
     or coalesce(v_fin.mileage_pay_ex_vat,0)<>0
     or coalesce(v_fin.mileage_charge_ex_vat,0)<>0
     or coalesce(v_fin.travel_pay_ex_vat,0)<>0
     or coalesce(v_fin.travel_charge_ex_vat,0)<>0
     or coalesce(v_fin.accommodation_pay_ex_vat,0)<>0
     or coalesce(v_fin.accommodation_charge_ex_vat,0)<>0
     or coalesce(v_fin.other_pay_ex_vat,0)<>0
     or coalesce(v_fin.other_charge_ex_vat,0)<>0
     or coalesce(v_fin.additional_pay_ex_vat,0)<>0
     or coalesce(v_fin.additional_charge_ex_vat,0)<>0
     or coalesce(v_fin.hours_day,0)<>0
     or coalesce(v_fin.hours_night,0)<>0
     or coalesce(v_fin.hours_sat,0)<>0
     or coalesce(v_fin.hours_sun,0)<>0
     or coalesce(v_fin.hours_bh,0)<>0
     or coalesce(v_fin.pay_day,0)<>0
     or coalesce(v_fin.pay_night,0)<>0
     or coalesce(v_fin.pay_sat,0)<>0
     or coalesce(v_fin.pay_sun,0)<>0
     or coalesce(v_fin.pay_bh,0)<>0
     or coalesce(v_fin.charge_day,0)<>0
     or coalesce(v_fin.charge_night,0)<>0
     or coalesce(v_fin.charge_sat,0)<>0
     or coalesce(v_fin.charge_sun,0)<>0
     or coalesce(v_fin.charge_bh,0)<>0
     or coalesce(v_fin.total_pay_ex_vat,0)<>0
     or coalesce(v_fin.total_charge_ex_vat,0)<>0
     or coalesce(v_fin.margin_ex_vat,0)<>0
     or coalesce(v_fin.expenses_pay_ex_vat,0)<>0
     or coalesce(v_fin.expenses_charge_ex_vat,0)<>0
     or coalesce(v_fin.additional_margin_ex_vat,0)<>0
     or coalesce(v_fin.pay_vat_amount_snapshot,0)<>0
     or coalesce(v_fin.pay_total_inc_vat_snapshot,0)<>0
     or v_fin.authorised_at_utc is not null
     or v_fin.locked_by_invoice_id is not null
     or v_fin.paid_at_utc is not null
     or v_timesheet.authorised_at_server is not null
     or upper(coalesce(v_timesheet.status::text,'')) in ('AUTHORISED','AUTHORIZED','INVOICED','PAID')
     or v_timesheet.sheet_scope::text<>'WEEKLY'
     or coalesce(v_timesheet.is_adjustment,false) is not true
     or upper(coalesce(v_timesheet.adjustment_origin,'')) in ('IMPORT_CORRECTION','IMPORT_CANCELLATION') then
    raise exception 'CANDIDATE_EXPENSE_CARRIER_NOT_EMPTY' using errcode='55000';
  end if;
  if exists(
    select 1 from public.candidate_expense_components component
    where component.lifecycle_state not in (
      'MANAGER_REFUSED','OFFICE_REJECTED','WITHDRAWN','CANCELLED','SUPERSEDED'
    ) and private._candidate_expense_owned_timesheet_id_v1(
      component.workflow_id,component.owning_timesheet_id
    )=p_timesheet_id
  ) then
    raise exception 'CANDIDATE_EXPENSE_CARRIER_HAS_LIVE_COMPONENTS' using errcode='55000';
  end if;
  if exists(
    select 1 from public.candidate_submission_workflows workflow
    where workflow.environment=v_environment
      and (workflow.target_timesheet_id=p_timesheet_id
        or workflow.anchor_timesheet_id=p_timesheet_id)
      and workflow.workflow_kind not in ('CONTRACT_EXPENSE','CONTRACT_COMBINED')
      and workflow.state not in ('CANCELLED','EXPIRED','SUPERSEDED','REJECTED','REFUSED')
  ) then
    raise exception 'CANDIDATE_EXPENSE_CARRIER_SHARED' using errcode='55000';
  end if;
  select defaults.candidate_app_system_actor_user_id into v_system_actor
  from public.settings_defaults defaults where defaults.id=1;
  if v_system_actor is null or not exists(
    select 1 from public.tms_users actor
    where actor.id=v_system_actor
  ) then
    raise exception 'CANDIDATE_SYSTEM_ACTOR_NOT_CONFIGURED' using errcode='55000';
  end if;
  v_preview:=public.timesheet_weekly_manual_adjustment_delete_preview(
    p_timesheet_id,v_system_actor
  );
  if coalesce(v_preview->>'decision','') not in (
    'PERMANENT_DELETE','ARCHIVE_REQUIRED'
  ) then
    raise exception 'CANDIDATE_EXPENSE_CARRIER_DELETE_BLOCKED'
      using errcode='55000',detail=v_preview::text;
  end if;
  select coalesce(array_agg(value::uuid order by value::uuid),array[]::uuid[])
  into v_timesheet_ids from jsonb_array_elements_text(
    coalesce(v_preview->'timesheet_ids','[]'::jsonb)
  ) ids(value);
  select coalesce(array_agg(value::uuid order by value::uuid),array[]::uuid[])
  into v_contract_week_ids from jsonb_array_elements_text(
    coalesce(v_preview->'contract_week_ids','[]'::jsonb)
  ) ids(value);
  select coalesce(array_agg(value::uuid order by value::uuid),array[]::uuid[])
  into v_source_timesheet_ids from jsonb_array_elements_text(
    coalesce(v_preview->'preserved_source_timesheet_ids','[]'::jsonb)
  ) ids(value);
  select coalesce(array_agg(value::uuid order by value::uuid),array[]::uuid[])
  into v_source_contract_week_ids from jsonb_array_elements_text(
    coalesce(v_preview->'preserved_source_contract_week_ids','[]'::jsonb)
  ) ids(value);
  if cardinality(v_timesheet_ids)=0 or not (p_timesheet_id=any(v_timesheet_ids)) then
    raise exception 'CANDIDATE_EXPENSE_CARRIER_DELETE_SCOPE_CHANGED' using errcode='40001';
  end if;
  if exists(
    select 1 from public.candidate_pending_expense_updates update_row
    where update_row.prior_paper_source_timesheet_id=any(v_timesheet_ids)
      and update_row.state in ('EDITING','RENDERING')
  ) then
    raise exception 'CANDIDATE_EXPENSE_UPDATE_IN_PROGRESS' using errcode='55000';
  end if;
  -- The established delete preview can expand to earlier booking versions.
  -- Recheck that complete scope before detaching any workflow or component;
  -- an expense carrier can never take a live hours/import workflow with it.
  if exists(
    select 1 from public.candidate_submission_workflows workflow
    where workflow.environment=v_environment
      and (workflow.target_timesheet_id=any(v_timesheet_ids)
        or workflow.anchor_timesheet_id=any(v_timesheet_ids))
      and workflow.workflow_kind not in ('CONTRACT_EXPENSE','CONTRACT_COMBINED')
      and workflow.state not in ('CANCELLED','EXPIRED','SUPERSEDED','REJECTED','REFUSED')
  ) then
    raise exception 'CANDIDATE_EXPENSE_CARRIER_SHARED' using errcode='55000';
  end if;
  if exists(
    select 1 from public.candidate_expense_components component
    where component.lifecycle_state not in (
      'MANAGER_REFUSED','OFFICE_REJECTED','WITHDRAWN','CANCELLED','SUPERSEDED'
    ) and private._candidate_expense_owned_timesheet_id_v1(
      component.workflow_id,component.owning_timesheet_id
    )=any(v_timesheet_ids)
  ) then
    raise exception 'CANDIDATE_EXPENSE_CARRIER_HAS_LIVE_COMPONENTS' using errcode='55000';
  end if;
  v_source_timesheet_id:=case when cardinality(v_source_timesheet_ids)>0
    then v_source_timesheet_ids[1] end;
  v_source_contract_week_id:=case when cardinality(v_source_contract_week_ids)>0
    then v_source_contract_week_ids[1] end;
  select public.timesheet_lifecycle_guard_signature_v1(
    p_timesheet_id,
    (select week.id from public.contract_weeks week
      where week.timesheet_id=p_timesheet_id order by week.id limit 1),false
  ) into v_signature;
  v_row_signature:=nullif(btrim(coalesce(
    v_signature->>'backend_row_signature',v_signature->>'row_signature',
    v_signature->>'signature',''
  )), '');
  if v_row_signature is null then
    raise exception 'CANDIDATE_EXPENSE_CARRIER_SIGNATURE_UNAVAILABLE' using errcode='55000';
  end if;
  v_empty_timesheet_consequence:=case
    when coalesce(v_preview->>'decision','')='ARCHIVE_REQUIRED'
      then 'REMOVE_FROM_CURRENT_KEEP_HISTORY'
    when coalesce(v_preview->>'decision','')='PERMANENT_DELETE'
      and v_source_contract_week_id is not null
      then 'PERMANENT_REMOVE'
    else 'NONE'
  end;
  if v_empty_timesheet_consequence<>'NONE' then
    v_delete_scope:=jsonb_build_object(
      'contract_version','CANDIDATE_EXPENSE_CARRIER_DELETE_SCOPE_V1',
      'requested_timesheet_id',p_timesheet_id,
      'current_timesheet_id',nullif(v_preview->>'current_timesheet_id','')::uuid,
      'timesheet_ids',to_jsonb(v_timesheet_ids),
      'contract_week_ids',to_jsonb(v_contract_week_ids),
      'preserved_source_timesheet_ids',to_jsonb(v_source_timesheet_ids),
      'preserved_source_contract_week_ids',to_jsonb(v_source_contract_week_ids),
      'row_signature',v_row_signature
    );
    v_delete_scope_sha256:=encode(
      private._candidate_sha256_jsonb_v1(v_delete_scope-'row_signature'),'hex'
    );
    begin
      v_expected_delete_scope_sha256:=nullif(current_setting(
        'cloudtms.candidate_expected_delete_scope_sha256',true
      ),'');
    exception when others then
      v_expected_delete_scope_sha256:=null;
    end;
    if v_expected_delete_scope_sha256='NO_DELETE' then
      raise exception 'CANDIDATE_EXPENSE_CATEGORY_CONTEXT_CHANGED'
        using errcode='40001',detail='Office confirmation did not permit removal of the empty carrier from current records';
    end if;
    if v_expected_delete_scope_sha256 is not null and (
      v_expected_delete_scope_sha256 !~ '^[0-9a-f]{64}$'
      or v_expected_delete_scope_sha256<>v_delete_scope_sha256
    ) then
      raise exception 'CANDIDATE_EXPENSE_CATEGORY_CONTEXT_CHANGED'
        using errcode='40001',detail=jsonb_build_object(
          'expected_delete_scope_sha256',v_expected_delete_scope_sha256,
          'current_delete_scope_sha256',v_delete_scope_sha256
        )::text;
    end if;
  end if;
  if coalesce(v_preview->>'decision','')='ARCHIVE_REQUIRED' then
    -- Sticky financial history forbids relational deletion, but the canonical
    -- archive transition safely removes the empty carrier from current Office
    -- views while retaining its exact Timesheet, contract-week and audit rows.
    v_result:=public.timesheet_archive_transition_v1(
      p_timesheet_id,'ARCHIVE','WEEKLY_MANUAL_ADJUSTMENT_DELETE',
      v_system_actor,p_timesheet_id,v_row_signature,p_now_utc
    );
    if not coalesce((v_result->>'ok')::boolean,false)
       or coalesce(v_result->>'decision','')<>'ARCHIVED'
       or coalesce(v_result->'timesheet_ids','[]'::jsonb)<>to_jsonb(v_timesheet_ids)
       or coalesce(v_result->'contract_week_ids','[]'::jsonb)<>to_jsonb(v_contract_week_ids) then
      raise exception 'CANDIDATE_EXPENSE_CARRIER_ARCHIVE_NOT_PROVEN'
        using errcode='40001',detail=coalesce(v_result,'{}'::jsonb)::text;
    end if;
    return jsonb_build_object(
      'ok',true,'owning_timesheet_deleted',false,
      'previous_owning_timesheet_id',p_timesheet_id,
      'empty_timesheet_consequence','REMOVE_FROM_CURRENT_KEEP_HISTORY',
      'deleted_timesheet_ids','[]'::jsonb,
      'retained_timesheet_ids',to_jsonb(v_timesheet_ids),
      'affected_timesheet_ids',to_jsonb(v_timesheet_ids),
      'removed_from_current_timesheet_ids',to_jsonb(v_timesheet_ids),
      'r2_cleanup_keys','[]'::jsonb,'idempotent_replay',false
    );
  end if;
  if v_source_contract_week_id is null then
    return jsonb_build_object(
      'ok',true,'owning_timesheet_deleted',false,
      'previous_owning_timesheet_id',p_timesheet_id,
      'empty_timesheet_consequence','NONE',
      'deleted_timesheet_ids','[]'::jsonb,
      'retained_timesheet_ids',jsonb_build_array(p_timesheet_id),
      'affected_timesheet_ids',jsonb_build_array(p_timesheet_id),
      'removed_from_current_timesheet_ids','[]'::jsonb,
      'r2_cleanup_keys','[]'::jsonb,'idempotent_replay',false
    );
  end if;

  -- Preserve a claimed render attempt through carrier deletion. Its late
  -- completion can then enqueue exact cleanup for the attempt-specific object
  -- rather than failing NOT_FOUND after an ON DELETE cascade.
  update public.candidate_expense_summary_refreshes refresh set
    state='SUPERSEDED',completed_at_utc=p_now_utc,updated_at_utc=p_now_utc,
    claim_token=case when refresh.state='RENDERING' then refresh.claim_token end,
    claimed_at_utc=case when refresh.state='RENDERING' then refresh.claimed_at_utc end,
    lease_expires_at_utc=case when refresh.state='RENDERING'
      then refresh.lease_expires_at_utc end
  where refresh.timesheet_id=any(v_timesheet_ids)
    and refresh.state in ('PENDING','RENDERING');

  update public.candidate_submission_components component set timesheet_id=null
  where component.timesheet_id=any(v_timesheet_ids);
  -- Stop an already-claimed stale deep link before detaching its deleted row.
  -- A terminal notice created by the current action uses p_now_utc exactly and
  -- remains live with its workflow-only destination below.
  update public.candidate_notifications notification set
    state='DISMISSED',dismissed_at_utc=coalesce(notification.dismissed_at_utc,p_now_utc),
    push_state=case when notification.push_state in ('PENDING','FAILED','CLAIMED')
      then 'SKIPPED' else notification.push_state end,
    last_error=case when notification.push_state in ('PENDING','FAILED','CLAIMED')
      then 'EXPENSE_CARRIER_DELETED' else notification.last_error end
  where notification.timesheet_id=any(v_timesheet_ids)
    and notification.created_at_utc<p_now_utc;
  update public.candidate_notifications notification set
    timesheet_id=null,
    deep_link_json=(coalesce(notification.deep_link_json,'{}'::jsonb)-'timesheet_id'-'contract_week_id')
      ||case when notification.workflow_id is null then '{}'::jsonb else
        jsonb_build_object('type','workflow','workflow_id',notification.workflow_id) end
  where notification.timesheet_id=any(v_timesheet_ids);
  update public.candidate_expense_components component set
    owning_timesheet_id=null,updated_at_utc=p_now_utc
  where component.owning_timesheet_id=any(v_timesheet_ids);
  update public.candidate_pending_expense_updates update_row set
    prior_paper_source_timesheet_id=null,updated_at_utc=p_now_utc
  where update_row.prior_paper_source_timesheet_id=any(v_timesheet_ids)
    and update_row.state in ('COMMITTED','ABORTED','FAILED');
  update public.candidate_submission_workflows workflow set
    state=case
      when v_source_contract_week_id is null
        and workflow.workflow_kind in ('CONTRACT_EXPENSE','CONTRACT_COMBINED')
        and workflow.state not in ('CANCELLED','EXPIRED','SUPERSEDED','REJECTED','REFUSED')
        and (workflow.target_timesheet_id=any(v_timesheet_ids)
          or workflow.anchor_timesheet_id=any(v_timesheet_ids)
          or workflow.contract_week_id=any(v_contract_week_ids))
      then 'CANCELLED'
      else workflow.state
    end,
    generation=case
      when v_source_contract_week_id is null
        and workflow.workflow_kind in ('CONTRACT_EXPENSE','CONTRACT_COMBINED')
        and workflow.state not in ('CANCELLED','EXPIRED','SUPERSEDED','REJECTED','REFUSED')
      then workflow.generation+1
      else workflow.generation
    end,
    cancelled_at_utc=case
      when v_source_contract_week_id is null
        and workflow.workflow_kind in ('CONTRACT_EXPENSE','CONTRACT_COMBINED')
        and workflow.state not in ('CANCELLED','EXPIRED','SUPERSEDED','REJECTED','REFUSED')
      then coalesce(workflow.cancelled_at_utc,p_now_utc)
      else workflow.cancelled_at_utc
    end,
    target_timesheet_id=case when workflow.target_timesheet_id=any(v_timesheet_ids)
      then null else workflow.target_timesheet_id end,
    anchor_timesheet_id=case when workflow.anchor_timesheet_id=any(v_timesheet_ids)
      then v_source_timesheet_id else workflow.anchor_timesheet_id end,
    contract_week_id=case when workflow.contract_week_id=any(v_contract_week_ids)
      then v_source_contract_week_id else workflow.contract_week_id end,
    input_snapshot_json=coalesce(workflow.input_snapshot_json,'{}'::jsonb)
      ||jsonb_build_object('expense_carrier_delete_tombstone',jsonb_build_object(
        'operation_id',p_operation_id,
        'deleted_timesheet_ids',to_jsonb(v_timesheet_ids),
        'deleted_contract_week_ids',to_jsonb(v_contract_week_ids),
        'previous_target_timesheet_id',workflow.target_timesheet_id,
        'previous_anchor_timesheet_id',workflow.anchor_timesheet_id,
        'previous_contract_week_id',workflow.contract_week_id,
        'retired_at_utc',p_now_utc
      )),
    issue_codes=(case when workflow.issue_codes @> '["EXPENSE_CARRIER_DELETED"]'::jsonb
      then workflow.issue_codes else workflow.issue_codes||'["EXPENSE_CARRIER_DELETED"]'::jsonb end)
      ||case
        when v_source_contract_week_id is null
          and workflow.workflow_kind in ('CONTRACT_EXPENSE','CONTRACT_COMBINED')
          and not (workflow.issue_codes @> '["OFFICE_PERMANENTLY_DELETED_TIMESHEET"]'::jsonb)
        then '["OFFICE_PERMANENTLY_DELETED_TIMESHEET"]'::jsonb
        else '[]'::jsonb
      end,
    updated_at_utc=p_now_utc
  where workflow.environment=v_environment and (
    workflow.target_timesheet_id=any(v_timesheet_ids)
    or workflow.anchor_timesheet_id=any(v_timesheet_ids)
    or workflow.contract_week_id=any(v_contract_week_ids)
  );

  perform set_config('cloudtms.candidate_expense_summary_suppressed','true',true);
  v_result:=public.timesheet_weekly_manual_adjustment_delete_apply(
    p_timesheet_id,v_system_actor,v_timesheet_ids,v_contract_week_ids,
    v_source_timesheet_ids,v_source_contract_week_ids,v_row_signature
  );
  if coalesce((v_result->>'apply_performed')::boolean,false) is not true
     or coalesce(v_result->>'decision','')<>'PERMANENT_DELETE' then
    raise exception 'CANDIDATE_EXPENSE_CARRIER_DELETE_NOT_PROVEN'
      using errcode='40001',detail=v_result::text;
  end if;
  -- The relational delete is already committed within this transaction, so
  -- every returned object key must have a durable, retryable cleanup row
  -- before this function may report success.  An immediate Worker delete may
  -- still complete these rows, but a crash cannot orphan the objects.
  select coalesce(jsonb_agg(jsonb_build_object(
    'r2_key',cleanup_key.value,
    'attempt_count',1,
    'error','POST_COMMIT_DELETE_PENDING'
  ) order by cleanup_key.value),'[]'::jsonb)
  into v_cleanup_failures
  from jsonb_array_elements_text(
    case when jsonb_typeof(v_result->'r2_cleanup_keys')='array'
      then v_result->'r2_cleanup_keys' else '[]'::jsonb end
  ) cleanup_key(value)
  where nullif(btrim(cleanup_key.value),'') is not null;
  if jsonb_array_length(v_cleanup_failures)>0 then
    v_cleanup_record:=public.timesheet_r2_cleanup_record_v1(
      'candidate-expense-carrier:'||p_operation_id::text,
      p_timesheet_id,
      v_timesheet_ids,
      v_cleanup_failures,
      null
    );
    if coalesce((v_cleanup_record->>'ok')::boolean,false) is not true
       or coalesce((v_cleanup_record->>'valid_distinct_keys')::integer,-1)
          is distinct from jsonb_array_length(v_cleanup_failures)
       or coalesce((v_cleanup_record->>'recorded')::integer,-1)
          is distinct from jsonb_array_length(v_cleanup_failures)
       or coalesce((v_cleanup_record->>'stale_or_complete_ignored')::integer,0)<>0 then
      raise exception 'CANDIDATE_EXPENSE_CARRIER_R2_CLEANUP_NOT_DURABLE'
        using errcode='40001',detail=coalesce(v_cleanup_record,'{}'::jsonb)::text;
    end if;
  end if;
  return v_result||jsonb_build_object(
    'owning_timesheet_deleted',true,
    'previous_owning_timesheet_id',p_timesheet_id,
    'empty_timesheet_consequence','PERMANENT_REMOVE',
    'retained_timesheet_ids',to_jsonb(v_source_timesheet_ids),
    'affected_timesheet_ids',to_jsonb(
      (select coalesce(array_agg(distinct id order by id),array[]::uuid[])
       from unnest(v_timesheet_ids||v_source_timesheet_ids) affected(id))
    ),
    'removed_from_current_timesheet_ids',to_jsonb(v_timesheet_ids),
    'r2_cleanup_durable',true
  );
end;
$function$;

-- Entering a printed-pack replacement is itself an exact, replayable
-- Candidate action.  The old frozen generation remains the recovery source;
-- the ordinary component upload/WORKER_SUBMIT path must carry update_id and
-- the Worker prepares one new complete PAPER pack before rebind commits it.
create or replace function public.candidate_expense_paper_update_begin_atomic_v1(
  p_session_id uuid,
  p_environment text,
  p_workflow_id uuid,
  p_expected_generation integer,
  p_category_changes jsonb,
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
  v_context jsonb;
  v_workflow public.candidate_submission_workflows%rowtype;
  v_operation public.candidate_expense_operations%rowtype;
  v_update public.candidate_pending_expense_updates%rowtype;
  v_request jsonb;
  v_request_sha bytea;
  v_begin jsonb;
  v_result jsonb;
begin
  if p_workflow_id is null or coalesce(p_expected_generation,0)<1
     or jsonb_typeof(p_category_changes) is distinct from 'array'
     or jsonb_array_length(p_category_changes) not between 1 and 4
     or nullif(pg_catalog.btrim(coalesce(p_idempotency_key,'')),'') is null then
    raise exception 'CANDIDATE_PAPER_DOCUMENT_UPDATE_INVALID' using errcode='22023';
  end if;
  v_context:=private._candidate_session_context_v1(
    p_session_id,v_environment,null,p_now_utc,true
  );
  -- The first successful BEGIN advances the workflow into a new draft
  -- generation.  Resolve its durable receipt by the original immutable
  -- request before consulting that mutable workflow state.
  v_request:=jsonb_build_object(
    'contract_version','CANDIDATE_PAPER_DOCUMENT_UPDATE_REQUEST_V1',
    'workflow_id',p_workflow_id,'workflow_generation',p_expected_generation,
    'category_changes',p_category_changes
  );
  v_request_sha:=private._candidate_sha256_jsonb_v1(v_request);
  perform pg_catalog.pg_advisory_xact_lock(pg_catalog.hashtextextended(
    'candidate-expense-operation|'||v_environment||'|'
      ||(v_context->>'selected_candidate_id')||'|'||pg_catalog.btrim(p_idempotency_key),0
  ));
  select operation.* into v_operation
  from public.candidate_expense_operations operation
  where operation.environment=v_environment
    and operation.actor_kind='CANDIDATE'
    and operation.actor_id=(v_context->>'selected_candidate_id')::uuid
    and operation.idempotency_key=pg_catalog.btrim(p_idempotency_key)
  for update;
  if found then
    if v_operation.action_code<>'CREATE_UPDATED_DOCUMENTS'
       or v_operation.workflow_id is distinct from p_workflow_id
       or v_operation.request_sha256<>v_request_sha then
      raise exception 'CANDIDATE_IDEMPOTENCY_CONFLICT' using errcode='23505';
    end if;
    if v_operation.state='COMMITTED' then
      return v_operation.result_json||jsonb_build_object('idempotent_replay',true);
    end if;
    if v_operation.state in ('FAILED','ABORTED') and v_operation.result_json is not null then
      return v_operation.result_json||jsonb_build_object('idempotent_replay',true);
    end if;
    if v_operation.state in ('PREPARING','RENDERING')
       and v_operation.progress_json is not null then
      return v_operation.progress_json||jsonb_build_object('idempotent_replay',true);
    end if;
    raise exception 'CANDIDATE_EXPENSE_OPERATION_IN_PROGRESS' using errcode='55000';
  end if;
  select workflow.* into v_workflow
  from public.candidate_submission_workflows workflow
  where workflow.id=p_workflow_id
    and workflow.environment=v_environment
    and workflow.account_id=(v_context->>'account_id')::uuid
    and workflow.candidate_id=(v_context->>'selected_candidate_id')::uuid
  for update;
  if not found then
    raise exception 'CANDIDATE_WORKFLOW_NOT_FOUND' using errcode='P0002';
  end if;
  if v_workflow.generation<>p_expected_generation
     or v_workflow.route<>'PAPER'
     or v_workflow.state<>'AWAITING_PAPER_RETURN'
     or v_workflow.workflow_kind not in ('CONTRACT_EXPENSE','CONTRACT_COMBINED') then
    raise exception 'CANDIDATE_PAPER_DOCUMENT_UPDATE_NOT_ALLOWED' using errcode='55000';
  end if;
  insert into public.candidate_expense_operations(
    environment,account_id,candidate_id,actor_kind,actor_id,action_code,
    workflow_id,timesheet_id,request_sha256,idempotency_key,state,
    created_at_utc,updated_at_utc
  ) values (
    v_environment,v_workflow.account_id,v_workflow.candidate_id,'CANDIDATE',
    v_workflow.candidate_id,'CREATE_UPDATED_DOCUMENTS',v_workflow.id,
    private._candidate_expense_current_timesheet_id_v1(
      v_workflow.id,coalesce(v_workflow.target_timesheet_id,v_workflow.anchor_timesheet_id)
    ),v_request_sha,pg_catalog.btrim(p_idempotency_key),'PREPARING',p_now_utc,p_now_utc
  ) returning * into v_operation;
  perform set_config(
    'cloudtms.candidate_paper_update_begin_context',
    jsonb_build_object(
      'purpose','CREATE_UPDATED_DOCUMENTS',
      'workflow_id',v_workflow.id,
      'workflow_generation',v_workflow.generation
    )::text,true
  );
  v_begin:=public.candidate_expense_update_begin_atomic_v1(
    p_session_id,v_environment,v_workflow.id,v_workflow.generation,
    p_category_changes,pg_catalog.btrim(p_idempotency_key),p_now_utc
  );
  perform set_config('cloudtms.candidate_paper_update_begin_context','',true);
  update public.candidate_pending_expense_updates update_row set
    operation_id=v_operation.operation_id,updated_at_utc=p_now_utc
  where update_row.update_id=(v_begin->>'update_id')::uuid
    and update_row.workflow_id=v_workflow.id and update_row.state='EDITING';
  if not found then
    raise exception 'CANDIDATE_EXPENSE_UPDATE_APPROVAL_CHANGED' using errcode='40001';
  end if;
  v_result:=jsonb_build_object(
    'contract_version','CANDIDATE_PAPER_DOCUMENT_UPDATE_RESULT_V1',
    'ok',true,'operation_id',v_operation.operation_id,
    'action_code','CREATE_UPDATED_DOCUMENTS','stage','EDITING',
    'workflow_id',v_workflow.id,'generation',(v_begin->>'generation')::integer,
    'state','WORKER_DRAFT','update_state','UPDATING',
    'update_id',(v_begin->>'update_id')::uuid,
    'category_changes',v_begin->'category_changes',
    'route','PAPER','route_selection_required',false,
    'upload_mode','EXISTING_WORKFLOW_DELTA',
    'submission_requires_update_id',true,
    'paper_pack_replacement',true,'old_pack_recoverable',true,
    'manager_link_preserved',false,'idempotent_replay',false
  );
  update public.candidate_expense_operations operation set
    progress_json=v_result,updated_at_utc=p_now_utc
  where operation.operation_id=v_operation.operation_id and operation.state='PREPARING';
  if not found then
    raise exception 'CANDIDATE_EXPENSE_OPERATION_CHANGED' using errcode='40001';
  end if;
  return v_result;
end;
$function$;

-- A refused category restarts as a blank category entry. If this linked
-- presentation already has its one permitted active expense workflow, the
-- blank category is added to that same manager request or unfinished PAPER
-- pack. Otherwise a new neutral expense draft is created and the Candidate
-- chooses PHONE, EMAIL or PAPER at the normal route step. No rejected value,
-- receipt or document is copied.
create or replace function public.candidate_expense_category_resubmit_atomic_v1(
  p_session_id uuid,
  p_environment text,
  p_source_workflow_id uuid,
  p_expected_generation integer,
  p_expense_component_id uuid,
  p_expected_component_generation integer,
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
  v_context jsonb;
  v_source public.candidate_submission_workflows%rowtype;
  v_component public.candidate_expense_components%rowtype;
  v_pending public.candidate_submission_workflows%rowtype;
  v_pending_update public.candidate_pending_expense_updates%rowtype;
  v_pending_count integer;
  v_request jsonb;
  v_request_sha bytea;
  v_operation public.candidate_expense_operations%rowtype;
  v_begin jsonb;
  v_create jsonb;
  v_result jsonb;
  v_new_workflow_id uuid;
  v_anchor_timesheet_id uuid;
begin
  if p_source_workflow_id is null or p_expected_generation is null
     or p_expense_component_id is null
     or p_expected_component_generation is null
     or nullif(pg_catalog.btrim(coalesce(p_idempotency_key,'')),'') is null then
    raise exception 'CANDIDATE_EXPENSE_CATEGORY_RESUBMISSION_INVALID'
      using errcode='22023';
  end if;
  v_context:=private._candidate_session_context_v1(
    p_session_id,v_environment,null,p_now_utc,true
  );
  -- Resolve an exact durable receipt before consulting mutable source state.
  -- The request body contains only caller-supplied immutable preconditions;
  -- category is a server projection/result fact, not part of the HTTP input.
  v_request:=jsonb_build_object(
    'contract_version','CANDIDATE_EXPENSE_CATEGORY_RESUBMISSION_REQUEST_V1',
    'source_workflow_id',p_source_workflow_id,
    'source_workflow_generation',p_expected_generation,
    'source_expense_component_id',p_expense_component_id,
    'source_component_generation',p_expected_component_generation
  );
  v_request_sha:=private._candidate_sha256_jsonb_v1(v_request);
  perform pg_catalog.pg_advisory_xact_lock(pg_catalog.hashtextextended(
    'candidate-expense-operation|'||v_environment||'|'
      ||(v_context->>'selected_candidate_id')||'|'||pg_catalog.btrim(p_idempotency_key),0
  ));
  select operation.* into v_operation
  from public.candidate_expense_operations operation
  where operation.environment=v_environment
    and operation.actor_kind='CANDIDATE'
    and operation.actor_id=(v_context->>'selected_candidate_id')::uuid
    and operation.idempotency_key=pg_catalog.btrim(p_idempotency_key)
  for update;
  if found then
    if v_operation.action_code<>'RESUBMIT_EXPENSE_CATEGORY'
       or v_operation.expense_component_id is distinct from p_expense_component_id
       or v_operation.request_sha256<>v_request_sha then
      raise exception 'CANDIDATE_IDEMPOTENCY_CONFLICT' using errcode='23505';
    end if;
    if v_operation.state='COMMITTED' then
      return v_operation.result_json||jsonb_build_object('idempotent_replay',true);
    end if;
    if v_operation.state in ('FAILED','ABORTED') and v_operation.result_json is not null then
      return v_operation.result_json||jsonb_build_object('idempotent_replay',true);
    end if;
    if v_operation.state in ('PREPARING','RENDERING') then
      select update_row.* into v_pending_update
      from public.candidate_pending_expense_updates update_row
      join public.candidate_submission_workflows workflow
        on workflow.id=update_row.workflow_id
      where update_row.operation_id=v_operation.operation_id
        and update_row.state in ('EDITING','RENDERING')
      limit 1;
      if found and v_operation.progress_json is not null then
        return v_operation.progress_json||jsonb_build_object(
          'generation',v_pending_update.current_workflow_generation,
          'state',case when v_pending_update.state='EDITING'
            then 'WORKER_DRAFT' else 'WORKER_SUBMITTED_PENDING_REVIEW_DOCUMENT' end,
          'idempotent_replay',true
        );
      end if;
    end if;
    raise exception 'CANDIDATE_EXPENSE_OPERATION_IN_PROGRESS' using errcode='55000';
  end if;
  select workflow.* into v_source
  from public.candidate_submission_workflows workflow
  where workflow.id=p_source_workflow_id
    and workflow.environment=v_environment
    and workflow.account_id=(v_context->>'account_id')::uuid
    and workflow.candidate_id=(v_context->>'selected_candidate_id')::uuid
  for update;
  if not found then
    raise exception 'CANDIDATE_WORKFLOW_NOT_FOUND' using errcode='P0002';
  end if;
  select component.* into v_component
  from public.candidate_expense_components component
  where component.expense_component_id=p_expense_component_id
    and component.workflow_id=v_source.id
  for update;
  if not found then
    raise exception 'CANDIDATE_EXPENSE_COMPONENT_NOT_FOUND' using errcode='P0002';
  end if;
  if v_source.generation<>p_expected_generation
     or v_component.component_generation<>p_expected_component_generation then
    raise exception 'CANDIDATE_EXPENSE_COMPONENT_CHANGED' using errcode='40001';
  end if;
  if v_component.lifecycle_state not in ('MANAGER_REFUSED','OFFICE_REJECTED')
     or v_component.agency_authorisation_state<>'NOT_AUTHORISED' then
    raise exception 'CANDIDATE_EXPENSE_CATEGORY_RESUBMISSION_NOT_ALLOWED'
      using errcode='55000';
  end if;
  perform pg_catalog.pg_advisory_xact_lock(pg_catalog.hashtextextended(
    'candidate-expense-pending|'||v_environment||'|'||v_source.candidate_id::text
      ||'|'||v_source.contract_id::text||'|'||v_source.week_ending_date::text,0
  ));
  select count(distinct workflow.id)::integer into v_pending_count
  from public.candidate_submission_workflows workflow
  where workflow.environment=v_environment
    and workflow.candidate_id=v_source.candidate_id
    and workflow.contract_id=v_source.contract_id
    and workflow.week_ending_date=v_source.week_ending_date
    and workflow.workflow_kind in ('CONTRACT_EXPENSE','CONTRACT_COMBINED')
    and (
      (
        workflow.route<>'PAPER'
        and workflow.state='AWAITING_MANAGER_APPROVAL'
        and exists(
          select 1 from public.candidate_approval_requests request
          where request.workflow_id=workflow.id
            and request.workflow_generation=workflow.generation
            and request.state='PENDING'
        )
      )
      or (workflow.route='PAPER' and workflow.state='AWAITING_PAPER_RETURN')
    );
  if v_pending_count>1 then
    raise exception 'CANDIDATE_EXPENSE_PENDING_WORKFLOW_CONFLICT' using errcode='55000';
  end if;
  if v_pending_count=1 then
    select workflow.* into v_pending
    from public.candidate_submission_workflows workflow
    where workflow.environment=v_environment
      and workflow.candidate_id=v_source.candidate_id
      and workflow.contract_id=v_source.contract_id
      and workflow.week_ending_date=v_source.week_ending_date
      and workflow.workflow_kind in ('CONTRACT_EXPENSE','CONTRACT_COMBINED')
      and (
        (
          workflow.route<>'PAPER'
          and workflow.state='AWAITING_MANAGER_APPROVAL'
          and exists(
            select 1 from public.candidate_approval_requests request
            where request.workflow_id=workflow.id
              and request.workflow_generation=workflow.generation
              and request.state='PENDING'
          )
        )
        or (workflow.route='PAPER' and workflow.state='AWAITING_PAPER_RETURN')
      )
    order by workflow.id limit 1 for update of workflow;
    if exists(
      select 1 from public.candidate_expense_components existing
      where existing.workflow_id=v_pending.id
        and existing.expense_category=v_component.expense_category
        and existing.lifecycle_state not in (
          'MANAGER_REFUSED','OFFICE_REJECTED','WITHDRAWN','CANCELLED','SUPERSEDED'
        )
    ) then
      raise exception 'CANDIDATE_EXPENSE_CATEGORY_ALREADY_PENDING' using errcode='55000';
    end if;
  end if;
  insert into public.candidate_expense_operations(
    environment,account_id,candidate_id,actor_kind,actor_id,action_code,
    workflow_id,timesheet_id,expense_component_id,request_sha256,idempotency_key,
    state,created_at_utc,updated_at_utc
  ) values (
    v_environment,v_source.account_id,v_source.candidate_id,'CANDIDATE',
    v_source.candidate_id,'RESUBMIT_EXPENSE_CATEGORY',v_source.id,
    private._candidate_expense_owned_timesheet_id_v1(
      v_source.id,v_component.owning_timesheet_id
    ),v_component.expense_component_id,v_request_sha,
    pg_catalog.btrim(p_idempotency_key),'PREPARING',p_now_utc,p_now_utc
  ) returning * into v_operation;

  if v_pending_count=1 then
    update public.candidate_expense_operations operation set
      workflow_id=v_pending.id,
      timesheet_id=private._candidate_expense_owned_timesheet_id_v1(
        v_pending.id,v_pending.target_timesheet_id
      ),
      updated_at_utc=p_now_utc
    where operation.operation_id=v_operation.operation_id
      and operation.state='PREPARING';
    if not found then
      raise exception 'CANDIDATE_EXPENSE_OPERATION_CHANGED' using errcode='40001';
    end if;
    select operation.* into v_operation
    from public.candidate_expense_operations operation
    where operation.operation_id=v_operation.operation_id for update;
    if v_pending.route='PAPER' then
      perform set_config(
        'cloudtms.candidate_paper_update_begin_context',
        jsonb_build_object(
          'purpose','RESUBMIT_EXPENSE_CATEGORY',
          'workflow_id',v_pending.id,
          'workflow_generation',v_pending.generation
        )::text,true
      );
    end if;
    v_begin:=public.candidate_expense_update_begin_atomic_v1(
      p_session_id,v_environment,v_pending.id,v_pending.generation,
      jsonb_build_array(jsonb_build_object(
        'update_kind','ADD_CATEGORY',
        'expense_category',v_component.expense_category
      )),
      'resubmit-category-begin:'||v_operation.operation_id::text,p_now_utc
    );
    if v_pending.route='PAPER' then
      perform set_config('cloudtms.candidate_paper_update_begin_context','',true);
    end if;
    update public.candidate_pending_expense_updates update_row set
      operation_id=v_operation.operation_id,updated_at_utc=p_now_utc
    where update_row.update_id=(v_begin->>'update_id')::uuid
      and update_row.state='EDITING';
    if not found then
      raise exception 'CANDIDATE_EXPENSE_UPDATE_APPROVAL_CHANGED' using errcode='40001';
    end if;
    v_result:=jsonb_build_object(
      'contract_version','CANDIDATE_EXPENSE_CATEGORY_RESUBMISSION_RESULT_V1',
      'ok',true,'operation_id',v_operation.operation_id,
      'action_code','RESUBMIT_EXPENSE_CATEGORY',
      'source_workflow_id',v_source.id,
      'source_expense_component_id',v_component.expense_component_id,
      'source_component_generation',v_component.component_generation,
      'expense_category',v_component.expense_category,
      'category_changes',jsonb_build_array(jsonb_build_object(
        'update_kind','ADD_CATEGORY',
        'expense_category',v_component.expense_category
      )),
      'editor_mode',case when v_pending.route='PAPER'
        then 'PAPER_REPLACEMENT' else 'PENDING_MANAGER_UPDATE' end,
      'workflow_id',(v_begin->>'workflow_id')::uuid,
      'generation',(v_begin->>'generation')::integer,
      'state','WORKER_DRAFT','update_state','UPDATING',
      'update_id',(v_begin->>'update_id')::uuid,'blank_claim',true,
      'route_selection_required',false,
      'route',case when v_pending.route='PAPER' then 'PAPER' else 'ELECTRONIC' end,
      'paper_pack_replacement',v_pending.route='PAPER',
      'old_pack_recoverable',v_pending.route='PAPER',
      'manager_link_preserved',v_pending.route<>'PAPER',
      'idempotent_replay',false
    );
    update public.candidate_expense_operations operation set
      progress_json=v_result,updated_at_utc=p_now_utc
    where operation.operation_id=v_operation.operation_id
      and operation.state='PREPARING';
    if not found then
      raise exception 'CANDIDATE_EXPENSE_OPERATION_CHANGED' using errcode='40001';
    end if;
    return v_result;
  end if;

  v_anchor_timesheet_id:=private._candidate_expense_current_timesheet_id_v1(
    v_source.id,v_source.anchor_timesheet_id
  );
  if v_anchor_timesheet_id is null and v_source.anchor_timesheet_id is not null
     and exists(
       select 1 from public.timesheets anchor
       where anchor.timesheet_id=v_source.anchor_timesheet_id
         and anchor.is_current and anchor.archived_at_utc is null
     ) then
    v_anchor_timesheet_id:=v_source.anchor_timesheet_id;
  end if;
  v_new_workflow_id:=pg_catalog.gen_random_uuid();
  v_create:=public.candidate_workflow_transition_atomic_v1(
    p_session_id,v_environment,v_new_workflow_id,'CREATE',null,
    jsonb_strip_nulls(jsonb_build_object(
      'workflow_kind','CONTRACT_EXPENSE','scope','WEEKLY','route','ELECTRONIC',
      'contract_id',v_source.contract_id,
      'contract_week_id',v_source.contract_week_id,
      'week_ending_date',v_source.week_ending_date,
      'anchor_timesheet_id',v_anchor_timesheet_id,
      'input_snapshot',jsonb_build_object(
        'expense_category_resubmission',jsonb_build_object(
          'source_workflow_id',v_source.id,
          'source_expense_component_id',v_component.expense_component_id,
          'expense_category',v_component.expense_category
        )
      )
    )),
    'resubmit-category-workflow:'||v_operation.operation_id::text,p_now_utc
  );
  if coalesce(v_create->>'state','')<>'WORKER_DRAFT' then
    raise exception 'CANDIDATE_EXPENSE_CATEGORY_RESUBMISSION_NOT_READY'
      using errcode='55000';
  end if;
  v_result:=jsonb_build_object(
    'contract_version','CANDIDATE_EXPENSE_CATEGORY_RESUBMISSION_RESULT_V1',
    'ok',true,'operation_id',v_operation.operation_id,
    'action_code','RESUBMIT_EXPENSE_CATEGORY',
    'source_workflow_id',v_source.id,
    'source_expense_component_id',v_component.expense_component_id,
    'source_component_generation',v_component.component_generation,
    'expense_category',v_component.expense_category,
    'category_changes',jsonb_build_array(jsonb_build_object(
      'update_kind','ADD_CATEGORY',
      'expense_category',v_component.expense_category
    )),
    'editor_mode','NEW_EXPENSE_CLAIM',
    'workflow_id',(v_create->>'workflow_id')::uuid,
    'generation',(v_create->>'generation')::integer,
    'state','WORKER_DRAFT','update_state','NONE','update_id',null,
    'blank_claim',true,'route_selection_required',true,
    'route',null,'paper_pack_replacement',false,'old_pack_recoverable',false,
    'manager_link_preserved',false,'idempotent_replay',false
  );
  update public.candidate_expense_operations operation set
    state='COMMITTED',result_json=v_result,completed_at_utc=p_now_utc,
    updated_at_utc=p_now_utc
  where operation.operation_id=v_operation.operation_id
    and operation.state='PREPARING';
  if not found then
    raise exception 'CANDIDATE_EXPENSE_OPERATION_CHANGED' using errcode='40001';
  end if;
  return v_result;
end;
$function$;

-- Persist the exact public PAPER replacement receipt after the new complete
-- pack is generated, bound to its held email and the old pack is retired.
-- Replaying WORKER_SUBMIT can therefore return the same COMPLETE receipt
-- after a lost HTTP response instead of an intermediate rebind result.
create or replace function public.candidate_expense_paper_update_receipt_commit_v1(
  p_environment text,
  p_operation_id uuid,
  p_update_id uuid,
  p_result jsonb,
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
  v_operation public.candidate_expense_operations%rowtype;
  v_update public.candidate_pending_expense_updates%rowtype;
  v_workflow public.candidate_submission_workflows%rowtype;
  v_manifest_sha256 text;
  v_page_count integer;
  v_result jsonb;
begin
  if p_operation_id is null or p_update_id is null
     or jsonb_typeof(p_result) is distinct from 'object' then
    raise exception 'CANDIDATE_PAPER_DOCUMENT_UPDATE_RECEIPT_INVALID'
      using errcode='22023';
  end if;
  select operation.* into v_operation
  from public.candidate_expense_operations operation
  where operation.operation_id=p_operation_id
    and operation.environment=v_environment
    and operation.actor_kind='CANDIDATE'
    and operation.action_code in (
      'CREATE_UPDATED_DOCUMENTS','RESUBMIT_EXPENSE_CATEGORY'
    )
  for update;
  if not found then
    raise exception 'CANDIDATE_EXPENSE_OPERATION_NOT_FOUND' using errcode='P0002';
  end if;
  select update_row.* into v_update
  from public.candidate_pending_expense_updates update_row
  where update_row.update_id=p_update_id
    and update_row.operation_id=v_operation.operation_id
    and update_row.workflow_id=v_operation.workflow_id
  for update;
  if not found then
    raise exception 'CANDIDATE_EXPENSE_UPDATE_NOT_FOUND' using errcode='P0002';
  end if;
  select workflow.* into v_workflow
  from public.candidate_submission_workflows workflow
  where workflow.id=v_operation.workflow_id
    and workflow.environment=v_environment
  for update;
  if not found then
    raise exception 'CANDIDATE_WORKFLOW_NOT_FOUND' using errcode='P0002';
  end if;
  if v_update.submit_result_json->>'contract_version'
       ='CANDIDATE_PAPER_DOCUMENT_UPDATE_RESULT_V1'
     and v_update.submit_result_json->>'stage'='COMPLETE' then
    return v_update.submit_result_json||jsonb_build_object('idempotent_replay',true);
  end if;
  if v_operation.state='COMMITTED'
     and v_operation.result_json->>'contract_version'
       ='CANDIDATE_PAPER_DOCUMENT_UPDATE_RESULT_V1'
     and v_operation.result_json->>'stage'='COMPLETE' then
    return v_operation.result_json||jsonb_build_object('idempotent_replay',true);
  end if;
  v_manifest_sha256:=encode(v_workflow.paper_return_manifest_sha256,'hex');
  v_page_count:=case
    when jsonb_typeof(v_workflow.paper_return_manifest_json->'pages')='array'
      then jsonb_array_length(v_workflow.paper_return_manifest_json->'pages')
    else 0 end;
  if v_operation.state<>'COMMITTED'
     or v_update.state<>'COMMITTED'
     or v_update.update_mode<>'PAPER_REPLACEMENT'
     or v_workflow.route<>'PAPER'
     or v_workflow.state<>'AWAITING_PAPER_RETURN'
     or v_workflow.generation<>v_update.current_workflow_generation
     or v_workflow.paper_return_manifest_sha256 is null
     or private._candidate_sha256_jsonb_v1(v_workflow.paper_return_manifest_json)
       is distinct from v_workflow.paper_return_manifest_sha256
     or coalesce(v_workflow.paper_return_manifest_json->>'manifest_version','')<>'2'
     or coalesce(v_workflow.paper_return_manifest_json->>'qr_contract_version','')
       <>'CANDIDATE_PAPER_PAGE_QR_V2'
     or v_page_count<1
     or coalesce(p_result->>'contract_version','')<>'CANDIDATE_PAPER_DOCUMENT_UPDATE_RESULT_V1'
     or coalesce(p_result->>'action_code','')<>'CREATE_UPDATED_DOCUMENTS'
     or coalesce(p_result->>'stage','')<>'COMPLETE'
     or coalesce((p_result->>'ok')::boolean,false) is not true
     or coalesce(p_result->>'operation_id','') !~
       '^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$'
     or (p_result->>'operation_id')::uuid is distinct from v_operation.operation_id
     or coalesce(p_result->>'workflow_id','') !~
       '^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$'
     or (p_result->>'workflow_id')::uuid is distinct from v_workflow.id
     or coalesce(p_result->>'update_id','') !~
       '^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$'
     or (p_result->>'update_id')::uuid is distinct from v_update.update_id
     or coalesce(p_result->>'generation','') !~ '^[1-9][0-9]*$'
     or (p_result->>'generation')::integer is distinct from v_workflow.generation
     or coalesce(p_result->>'state','')<>'AWAITING_PAPER_RETURN'
     or coalesce(p_result->>'update_state','')<>'NONE'
     or coalesce(p_result->>'route','')<>'PAPER'
     or coalesce((p_result->>'paper_pack_replacement')::boolean,false) is not true
     or coalesce((p_result->>'old_pack_recoverable')::boolean,true) is not false
     or coalesce((p_result->>'old_pack_retired')::boolean,false) is not true
     or coalesce((p_result->>'manager_link_preserved')::boolean,true) is not false
     or lower(coalesce(p_result->>'paper_return_manifest_sha256',''))<>v_manifest_sha256
     or coalesce(p_result->>'paper_return_page_count','') !~ '^[1-9][0-9]*$'
     or (p_result->>'paper_return_page_count')::integer is distinct from v_page_count
     or coalesce(p_result->>'paper_return_manifest_version','') !~ '^[1-9][0-9]*$'
     or (p_result->>'paper_return_manifest_version')::integer is distinct from 2
     or coalesce(p_result->>'paper_return_qr_contract_version','')<>'CANDIDATE_PAPER_PAGE_QR_V2'
     or coalesce((p_result->>'paper_pack_queued')::boolean,false) is not true
     or coalesce((p_result->>'paper_pack_email_bound')::boolean,false) is not true
     or jsonb_typeof(p_result->'paper_pack') is distinct from 'object'
     or jsonb_typeof(p_result->'paper_return_pages') is distinct from 'array'
     or jsonb_array_length(p_result->'paper_return_pages')<>v_page_count
     or jsonb_typeof(p_result->'category_changes') is distinct from 'array' then
    raise exception 'CANDIDATE_PAPER_DOCUMENT_UPDATE_RECEIPT_INVALID'
      using errcode='40001';
  end if;
  v_result:=p_result||jsonb_build_object('idempotent_replay',false);
  if v_operation.action_code='CREATE_UPDATED_DOCUMENTS' then
    update public.candidate_expense_operations operation set
      result_json=v_result,progress_json=null,updated_at_utc=p_now_utc
    where operation.operation_id=v_operation.operation_id
      and operation.state='COMMITTED';
  end if;
  update public.candidate_pending_expense_updates update_row set
    submit_result_json=v_result,updated_at_utc=p_now_utc
  where update_row.update_id=v_update.update_id
    and update_row.state='COMMITTED';
  return v_result;
end;
$function$;

-- Office PAPER/QR category rejection uses the same immutable pack builder as
-- Candidate replacement, but the service identity is opened and closed inside
-- one database call.  The operation/update binding prevents this capability
-- from being reused for an unrelated workflow or an ordinary manual record.
create or replace function public.candidate_office_expense_paper_prepare_v1(
  p_actor_user_id uuid,
  p_environment text,
  p_operation_id uuid,
  p_update_id uuid,
  p_workflow_id uuid,
  p_expected_generation integer,
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
  v_operation public.candidate_expense_operations%rowtype;
  v_update public.candidate_pending_expense_updates%rowtype;
  v_workflow public.candidate_submission_workflows%rowtype;
  v_target jsonb;
  v_result jsonb;
begin
  if p_actor_user_id is null or p_operation_id is null or p_update_id is null
     or p_workflow_id is null or coalesce(p_expected_generation,0)<1
     or nullif(btrim(coalesce(p_idempotency_key,'')),'') is null then
    raise exception 'CANDIDATE_EXPENSE_PAPER_REPLACEMENT_NOT_READY' using errcode='22023';
  end if;
  select operation.* into v_operation
  from public.candidate_expense_operations operation
  where operation.operation_id=p_operation_id
    and operation.environment=v_environment
    and operation.actor_kind='OFFICE'
    and operation.actor_id=p_actor_user_id
    and operation.action_code='REJECT_EXPENSE_CATEGORY'
    and operation.workflow_id=p_workflow_id
    and operation.state='RENDERING'
  for update;
  select update_row.* into v_update
  from public.candidate_pending_expense_updates update_row
  where update_row.update_id=p_update_id
    and update_row.operation_id=p_operation_id
    and update_row.workflow_id=p_workflow_id
    and update_row.update_mode='PAPER_REPLACEMENT'
    and update_row.state='RENDERING'
  for update;
  select workflow.* into v_workflow
  from public.candidate_submission_workflows workflow
  where workflow.id=p_workflow_id and workflow.environment=v_environment
    and workflow.generation=p_expected_generation
  for update;
  if v_operation.operation_id is null or v_update.update_id is null
     or v_workflow.id is null
     or v_update.current_workflow_generation<>p_expected_generation then
    raise exception 'CANDIDATE_EXPENSE_PAPER_REPLACEMENT_NOT_READY' using errcode='40001';
  end if;
  perform private._candidate_office_service_context_open_v1(
    v_environment,p_actor_user_id,'reject_submission','REJECT_EXPENSE_CATEGORY',p_now_utc
  );
  if v_workflow.workflow_kind<>'CONTRACT_EXPENSE' then
    v_target:=public.candidate_weekly_paper_target_prepare_v1(
      null,v_environment,v_workflow.id,v_workflow.generation,p_now_utc
    );
    if coalesce((v_target->>'ok')::boolean,false) is not true then
      raise exception 'CANDIDATE_PAPER_TIMESHEET_NOT_READY' using errcode='40001';
    end if;
  end if;
  v_result:=public.candidate_weekly_paper_prepare_atomic_v1(
    null,v_environment,v_workflow.id,'PAPER_PREPARE',v_workflow.generation,
    jsonb_build_object(
      'service_office_action',true,'actor_user_id',p_actor_user_id,
      'expense_update_id',v_update.update_id,
      'expense_operation_id',v_operation.operation_id
    ),btrim(p_idempotency_key),p_now_utc
  );
  perform private._candidate_office_service_context_close_v1();
  return v_result;
exception when others then
  perform private._candidate_office_service_context_close_v1();
  raise;
end;
$function$;

create or replace function public.candidate_office_expense_paper_promote_v1(
  p_actor_user_id uuid,
  p_environment text,
  p_operation_id uuid,
  p_update_id uuid,
  p_workflow_id uuid,
  p_expected_generation integer,
  p_expected_v1_manifest_sha256_hex text,
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
  v_operation public.candidate_expense_operations%rowtype;
  v_update public.candidate_pending_expense_updates%rowtype;
  v_result jsonb;
begin
  select operation.* into v_operation
  from public.candidate_expense_operations operation
  where operation.operation_id=p_operation_id
    and operation.environment=v_environment
    and operation.actor_kind='OFFICE' and operation.actor_id=p_actor_user_id
    and operation.action_code='REJECT_EXPENSE_CATEGORY'
    and operation.workflow_id=p_workflow_id and operation.state='RENDERING'
  for update;
  select update_row.* into v_update
  from public.candidate_pending_expense_updates update_row
  where update_row.update_id=p_update_id and update_row.operation_id=p_operation_id
    and update_row.workflow_id=p_workflow_id
    and update_row.update_mode='PAPER_REPLACEMENT'
    and update_row.state='RENDERING'
  for update;
  if v_operation.operation_id is null or v_update.update_id is null
     or v_update.current_workflow_generation<>p_expected_generation then
    raise exception 'CANDIDATE_EXPENSE_PAPER_REPLACEMENT_NOT_READY' using errcode='40001';
  end if;
  perform private._candidate_office_service_context_open_v1(
    v_environment,p_actor_user_id,'reject_submission','REJECT_EXPENSE_CATEGORY',p_now_utc
  );
  v_result:=public.candidate_paper_manifest_v2_promote_v1(
    null,v_environment,p_workflow_id,p_expected_generation,
    p_expected_v1_manifest_sha256_hex,p_now_utc
  );
  perform private._candidate_office_service_context_close_v1();
  return v_result;
exception when others then
  perform private._candidate_office_service_context_close_v1();
  raise;
end;
$function$;

alter function private._candidate_expense_summary_queue_v1(uuid,timestamptz) owner to postgres;
alter function private._candidate_zero_expense_carrier_delete_v1(text,uuid,uuid,timestamptz) owner to postgres;
alter function public.candidate_expense_paper_update_begin_atomic_v1(uuid,text,uuid,integer,jsonb,text,timestamptz) owner to postgres;
alter function public.candidate_expense_category_resubmit_atomic_v1(uuid,text,uuid,integer,uuid,integer,text,timestamptz) owner to postgres;
alter function public.candidate_expense_paper_update_receipt_commit_v1(text,uuid,uuid,jsonb,timestamptz) owner to postgres;
alter function public.candidate_office_expense_paper_prepare_v1(uuid,text,uuid,uuid,uuid,integer,text,timestamptz) owner to postgres;
alter function public.candidate_office_expense_paper_promote_v1(uuid,text,uuid,uuid,uuid,integer,text,timestamptz) owner to postgres;
alter function public.candidate_expense_summary_claim_v1(integer,integer,timestamptz) owner to postgres;
alter function public.candidate_expense_summary_render_begin_v1(uuid,uuid,text,text,timestamptz) owner to postgres;
alter function public.candidate_expense_summary_complete_v1(uuid,uuid,text,text,text,timestamptz) owner to postgres;
alter function public.candidate_expense_summary_fail_v1(uuid,uuid,text,text,timestamptz) owner to postgres;

revoke all on function private._candidate_expense_summary_queue_v1(uuid,timestamptz)
  from public,anon,authenticated,service_role;
revoke all on function private._candidate_zero_expense_carrier_delete_v1(text,uuid,uuid,timestamptz)
  from public,anon,authenticated,service_role;
revoke all on function public.candidate_expense_paper_update_begin_atomic_v1(uuid,text,uuid,integer,jsonb,text,timestamptz)
  from public,anon,authenticated,service_role;
revoke all on function public.candidate_expense_category_resubmit_atomic_v1(uuid,text,uuid,integer,uuid,integer,text,timestamptz)
  from public,anon,authenticated,service_role;
revoke all on function public.candidate_expense_paper_update_receipt_commit_v1(text,uuid,uuid,jsonb,timestamptz)
  from public,anon,authenticated,service_role;
revoke all on function public.candidate_office_expense_paper_prepare_v1(uuid,text,uuid,uuid,uuid,integer,text,timestamptz)
  from public,anon,authenticated,service_role;
revoke all on function public.candidate_office_expense_paper_promote_v1(uuid,text,uuid,uuid,uuid,integer,text,timestamptz)
  from public,anon,authenticated,service_role;
revoke all on function public.candidate_expense_summary_claim_v1(integer,integer,timestamptz)
  from public,anon,authenticated;
revoke all on function public.candidate_expense_summary_render_begin_v1(uuid,uuid,text,text,timestamptz)
  from public,anon,authenticated;
revoke all on function public.candidate_expense_summary_complete_v1(uuid,uuid,text,text,text,timestamptz)
  from public,anon,authenticated;
revoke all on function public.candidate_expense_summary_fail_v1(uuid,uuid,text,text,timestamptz)
  from public,anon,authenticated;
grant execute on function public.candidate_expense_summary_claim_v1(integer,integer,timestamptz)
  to service_role;
grant execute on function public.candidate_expense_summary_render_begin_v1(uuid,uuid,text,text,timestamptz)
  to service_role;
grant execute on function public.candidate_expense_summary_complete_v1(uuid,uuid,text,text,text,timestamptz)
  to service_role;
grant execute on function public.candidate_expense_summary_fail_v1(uuid,uuid,text,text,timestamptz)
  to service_role;
grant execute on function public.candidate_expense_paper_update_begin_atomic_v1(uuid,text,uuid,integer,jsonb,text,timestamptz)
  to service_role;
grant execute on function public.candidate_expense_category_resubmit_atomic_v1(uuid,text,uuid,integer,uuid,integer,text,timestamptz)
  to service_role;
grant execute on function public.candidate_expense_paper_update_receipt_commit_v1(text,uuid,uuid,jsonb,timestamptz)
  to service_role;
grant execute on function public.candidate_office_expense_paper_prepare_v1(uuid,text,uuid,uuid,uuid,integer,text,timestamptz)
  to service_role;
grant execute on function public.candidate_office_expense_paper_promote_v1(uuid,text,uuid,uuid,uuid,integer,text,timestamptz)
  to service_role;

comment on function public.candidate_expense_summary_claim_v1(integer,integer,timestamptz) is
  'Claims immutable expense-summary generations. A later financial edit supersedes rather than mutates a claimed render.';
comment on function public.candidate_expense_summary_complete_v1(uuid,uuid,text,text,text,timestamptz) is
  'Publishes one exact current unsigned internal Expense Summary only when the claimed totals generation remains current.';
comment on function public.candidate_expense_paper_update_begin_atomic_v1(uuid,text,uuid,integer,jsonb,text,timestamptz) is
  'Begins one replayable complete PAPER-pack replacement while retaining the prior immutable generation as the abort authority.';
comment on function public.candidate_expense_paper_update_receipt_commit_v1(text,uuid,uuid,jsonb,timestamptz) is
  'Persists the exact COMPLETE public receipt for a successfully generated and retired PAPER-pack replacement.';

notify pgrst, 'reload schema';

commit;
