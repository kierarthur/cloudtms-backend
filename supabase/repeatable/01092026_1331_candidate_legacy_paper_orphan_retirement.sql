-- Candidate cancellation compatibility for one exact retired PAPER shape.
--
-- The historical one-page pack pre-dates manifest v2.  Some installed packs
-- retained a current Timesheet QR token after the sole sent delivery receipt
-- had recorded a different token hash.  The ordinary retirement owner must
-- continue to reject that mismatch.  This adapter first executes the ordinary
-- cancellation and opens the compatibility path only for its exact
-- CURRENT_QR_TOKEN_OWNER_CONFLICT/owner_count=0 result.  Every compatibility
-- mutation and the ordinary cancellation remain in one database transaction.

\set ON_ERROR_STOP on

begin;

create or replace function private._candidate_legacy_paper_orphan_prepare_v1(
  p_environment text,
  p_workflow_id uuid,
  p_expected_generation integer,
  p_now_utc timestamptz default now()
)
returns jsonb
language plpgsql
security definer
set search_path = pg_catalog, public, private, extensions, pg_temp
as $function$
declare
  v_environment text:=private._candidate_assert_environment(p_environment);
  v_now_utc timestamptz:=coalesce(p_now_utc,now());
  v_workflow public.candidate_submission_workflows%rowtype;
  v_mail public.mail_outbox%rowtype;
  v_mail_id uuid;
  v_source public.timesheets%rowtype;
  v_source_key text;
  v_receipt_count integer:=0;
  v_current_source_count integer:=0;
  v_nonterminal_count integer:=0;
  v_selected_nonterminal_count integer:=0;
  v_current_token_owner_count integer:=0;
  v_receipt_token_hash text;
  v_current_token_hash text;
begin
  if p_workflow_id is null or coalesce(p_expected_generation,0)<1 then
    raise exception 'CANDIDATE_LEGACY_PAPER_ORPHAN_CONTEXT_INVALID'
      using errcode='22023';
  end if;

  select workflow.* into v_workflow
  from public.candidate_submission_workflows workflow
  where workflow.id=p_workflow_id
  for update;
  if not found then
    raise exception 'CANDIDATE_WORKFLOW_NOT_FOUND' using errcode='P0002';
  end if;
  if v_workflow.environment<>v_environment then
    raise exception 'CANDIDATE_ENVIRONMENT_MISMATCH' using errcode='28000';
  end if;
  if v_workflow.generation<>p_expected_generation then
    raise exception 'WORKFLOW_GENERATION_CONFLICT' using errcode='40001';
  end if;

  -- Only the retired, one-page, pre-v2 Weekly hours pack is eligible.  A v2
  -- page manifest, an expense pack, a returned pack or a finalised pack can
  -- never be repaired by this compatibility path.
  if v_workflow.route<>'PAPER'
     or v_workflow.state<>'AWAITING_PAPER_RETURN'
     or v_workflow.scope<>'WEEKLY'
     or v_workflow.workflow_kind<>'CONTRACT_HOURS'
     or v_workflow.paper_return_manifest_sha256 is null
     or jsonb_typeof(v_workflow.paper_return_manifest_json)<>'object'
     or nullif(btrim(coalesce(
       v_workflow.paper_return_manifest_json->>'manifest_version',''
     )),'') is not null
     or jsonb_typeof(v_workflow.paper_return_manifest_json->'pages')<>'array'
     or jsonb_array_length(v_workflow.paper_return_manifest_json->'pages')<>1
     or v_workflow.paper_return_manifest_json#>>'{pages,0,page_key}'<>'HOURS_TIMESHEET'
     or v_workflow.paper_return_manifest_json#>>'{pages,0,component_kind}'<>'HOURS_TIMESHEET'
     or private._candidate_sha256_jsonb_v1(v_workflow.paper_return_manifest_json)
          is distinct from v_workflow.paper_return_manifest_sha256 then
    raise exception 'CANDIDATE_LEGACY_PAPER_ORPHAN_NOT_ELIGIBLE'
      using errcode='40001',detail=jsonb_build_object(
        'code','CANDIDATE_LEGACY_PAPER_ORPHAN_NOT_ELIGIBLE',
        'reason','LEGACY_ONE_PAGE_MANIFEST_REQUIRED'
      )::text;
  end if;

  select count(*)::integer,
    (array_agg(mail_row.id order by mail_row.id))[1]
  into v_receipt_count,v_mail_id
  from public.mail_outbox mail_row
  where mail_row.type='TIMESHEET_QR'
    and mail_row.context_kind='timesheets'
    and mail_row.payment_scope_json->>'candidate_workflow_id'=v_workflow.id::text
    and mail_row.payment_scope_json->>'candidate_workflow_generation'=
      v_workflow.generation::text;
  if v_receipt_count<>1 or v_mail_id is null then
    raise exception 'CANDIDATE_LEGACY_PAPER_ORPHAN_NOT_ELIGIBLE'
      using errcode='40001',detail=jsonb_build_object(
        'code','CANDIDATE_LEGACY_PAPER_ORPHAN_NOT_ELIGIBLE',
        'reason','EXACTLY_ONE_DELIVERY_RECEIPT_REQUIRED',
        'receipt_count',v_receipt_count
      )::text;
  end if;

  select mail_row.* into v_mail
  from public.mail_outbox mail_row
  where mail_row.id=v_mail_id
  for update;
  if v_mail.context_id is null
     or v_mail.context_id is distinct from coalesce(
       v_workflow.target_timesheet_id,v_workflow.anchor_timesheet_id
     )
     or v_mail.status<>'SENT'
     or v_mail.payment_scope_json->>'candidate_mail_authority'<>'CANDIDATE_PAPER_V1'
     or lower(coalesce(v_mail.payment_scope_json->>'candidate_paper_pack_ready','false'))
          not in ('true','t','1','yes')
     or lower(coalesce(
       v_mail.payment_scope_json->>'candidate_paper_generation_retired','false'
     )) not in ('false','f','0','no')
     or lower(coalesce(v_mail.payment_scope_json->>'paper_return_manifest_sha256',''))
          <>encode(v_workflow.paper_return_manifest_sha256,'hex')
     or (
       nullif(btrim(coalesce(v_mail.attempt_lease_token,'')),'') is not null
       and (v_mail.attempt_lease_expires_at_utc is null
         or v_mail.attempt_lease_expires_at_utc>v_now_utc)
     ) then
    raise exception 'CANDIDATE_LEGACY_PAPER_ORPHAN_NOT_ELIGIBLE'
      using errcode='40001',detail=jsonb_build_object(
        'code','CANDIDATE_LEGACY_PAPER_ORPHAN_NOT_ELIGIBLE',
        'reason','DELIVERY_RECEIPT_NOT_EXACT_LEGACY_SENT_PACK'
      )::text;
  end if;

  v_receipt_token_hash:=lower(btrim(coalesce(
    v_mail.payment_scope_json->>'qr_token_hash',''
  )));
  if v_receipt_token_hash !~ '^[0-9a-f]{64}$' then
    raise exception 'CANDIDATE_LEGACY_PAPER_ORPHAN_NOT_ELIGIBLE'
      using errcode='40001',detail=jsonb_build_object(
        'code','CANDIDATE_LEGACY_PAPER_ORPHAN_NOT_ELIGIBLE',
        'reason','DELIVERY_RECEIPT_TOKEN_HASH_INVALID'
      )::text;
  end if;

  select source_row.* into v_source
  from public.timesheets source_row
  where source_row.timesheet_id=v_mail.context_id
  for update;
  if not found
     or not v_source.is_current
     or v_source.archived_at_utc is not null
     or v_source.contract_id is distinct from v_workflow.contract_id
     or v_source.week_ending_date is distinct from v_workflow.week_ending_date
     or upper(coalesce(v_source.line_type::text,'')) in ('EXPENSES','MILEAGE')
     or nullif(btrim(coalesce(v_source.qr_token,'')),'') is null then
    raise exception 'CANDIDATE_LEGACY_PAPER_ORPHAN_NOT_ELIGIBLE'
      using errcode='40001',detail=jsonb_build_object(
        'code','CANDIDATE_LEGACY_PAPER_ORPHAN_NOT_ELIGIBLE',
        'reason','CURRENT_SOURCE_NOT_EXACT'
      )::text;
  end if;

  v_source_key:=case
    when nullif(btrim(coalesce(v_source.booking_id,'')),'') is not null
      then 'BOOKING:'||v_source.booking_id
    else 'TIMESHEET:'||v_source.timesheet_id::text
  end;
  perform pg_advisory_xact_lock(hashtextextended(
    'CANDIDATE_PAPER_SOURCE:'||v_source_key,0
  ));

  if v_source_key like 'BOOKING:%' then
    select count(*)::integer into v_current_source_count
    from public.timesheets current_source
    where current_source.booking_id=substring(v_source_key from 9)
      and current_source.is_current=true
      and current_source.archived_at_utc is null
      and upper(coalesce(current_source.line_type::text,'')) not in ('EXPENSES','MILEAGE');
  else
    v_current_source_count:=1;
  end if;
  if v_current_source_count<>1 then
    raise exception 'CANDIDATE_LEGACY_PAPER_ORPHAN_NOT_ELIGIBLE'
      using errcode='40001',detail=jsonb_build_object(
        'code','CANDIDATE_LEGACY_PAPER_ORPHAN_NOT_ELIGIBLE',
        'reason','CURRENT_SOURCE_CARDINALITY_CONFLICT',
        'current_source_count',v_current_source_count
      )::text;
  end if;

  -- Re-prove that the selected workflow is the sole live/returnable workflow
  -- on the source and that no current workflow owns the retained token.
  select count(distinct relevant_workflow.id)::integer,
    count(distinct relevant_workflow.id) filter (
      where relevant_workflow.id=v_workflow.id
        and relevant_workflow.generation=v_workflow.generation
    )::integer
  into v_nonterminal_count,v_selected_nonterminal_count
  from public.candidate_submission_workflows relevant_workflow
  join public.mail_outbox relevant_mail
    on relevant_mail.type='TIMESHEET_QR'
   and relevant_mail.context_kind='timesheets'
   and relevant_mail.payment_scope_json->>'candidate_workflow_id'=
        relevant_workflow.id::text
   and relevant_mail.payment_scope_json->>'candidate_workflow_generation'=
        relevant_workflow.generation::text
  join public.timesheets mail_source
    on mail_source.timesheet_id=relevant_mail.context_id
  where relevant_workflow.route='PAPER'
    and relevant_workflow.state in ('AWAITING_PAPER_RETURN','RECEIVED')
    and (case
      when nullif(btrim(coalesce(mail_source.booking_id,'')),'') is not null
        then 'BOOKING:'||mail_source.booking_id
      else 'TIMESHEET:'||mail_source.timesheet_id::text
    end)=v_source_key;
  if v_nonterminal_count<>1 or v_selected_nonterminal_count<>1 then
    raise exception 'CANDIDATE_LEGACY_PAPER_ORPHAN_NOT_ELIGIBLE'
      using errcode='40001',detail=jsonb_build_object(
        'code','CANDIDATE_LEGACY_PAPER_ORPHAN_NOT_ELIGIBLE',
        'reason','NONTERMINAL_WORKFLOW_CARDINALITY_CONFLICT',
        'nonterminal_count',v_nonterminal_count
      )::text;
  end if;

  if exists(
    select 1
    from public.mail_outbox leased_mail
    join public.candidate_submission_workflows leased_workflow
      on leased_workflow.id::text=
        leased_mail.payment_scope_json->>'candidate_workflow_id'
    join public.timesheets mail_source
      on mail_source.timesheet_id=leased_mail.context_id
    where leased_mail.type='TIMESHEET_QR'
      and leased_mail.context_kind='timesheets'
      and leased_workflow.route='PAPER'
      and leased_workflow.state in ('AWAITING_PAPER_RETURN','RECEIVED','FINALISED')
      and (case
        when nullif(btrim(coalesce(mail_source.booking_id,'')),'') is not null
          then 'BOOKING:'||mail_source.booking_id
        else 'TIMESHEET:'||mail_source.timesheet_id::text
      end)=v_source_key
      and leased_mail.status<>'SENT'
      and nullif(btrim(coalesce(leased_mail.attempt_lease_token,'')),'') is not null
      and (leased_mail.attempt_lease_expires_at_utc is null
        or leased_mail.attempt_lease_expires_at_utc>v_now_utc)
  ) then
    raise exception 'CANDIDATE_PAPER_MAIL_DELIVERY_IN_PROGRESS'
      using errcode='40001';
  end if;

  v_current_token_hash:=encode(extensions.digest(
    convert_to(v_source.qr_token,'UTF8'),'sha256'
  ),'hex');
  if v_current_token_hash=v_receipt_token_hash then
    raise exception 'CANDIDATE_LEGACY_PAPER_ORPHAN_NOT_ELIGIBLE'
      using errcode='40001',detail=jsonb_build_object(
        'code','CANDIDATE_LEGACY_PAPER_ORPHAN_NOT_ELIGIBLE',
        'reason','ORDINARY_TOKEN_OWNER_PATH_REQUIRED'
      )::text;
  end if;

  select count(*)::integer into v_current_token_owner_count
  from (
    select distinct relevant_workflow.id
    from public.mail_outbox owner_mail
    join public.candidate_submission_workflows relevant_workflow
      on relevant_workflow.id::text=
        owner_mail.payment_scope_json->>'candidate_workflow_id'
    join public.timesheets mail_source
      on mail_source.timesheet_id=owner_mail.context_id
    where owner_mail.type='TIMESHEET_QR'
      and owner_mail.context_kind='timesheets'
      and relevant_workflow.route='PAPER'
      and relevant_workflow.state in ('AWAITING_PAPER_RETURN','RECEIVED','FINALISED')
      and owner_mail.payment_scope_json->>'candidate_workflow_generation'=
        (case when relevant_workflow.state='FINALISED'
          then greatest(relevant_workflow.generation-1,1)
          else relevant_workflow.generation end)::text
      and lower(coalesce(owner_mail.payment_scope_json->>'qr_token_hash',''))=
        v_current_token_hash
      and (case
        when nullif(btrim(coalesce(mail_source.booking_id,'')),'') is not null
          then 'BOOKING:'||mail_source.booking_id
        else 'TIMESHEET:'||mail_source.timesheet_id::text
      end)=v_source_key
  ) current_owner;
  if v_current_token_owner_count<>0 then
    raise exception 'CANDIDATE_LEGACY_PAPER_ORPHAN_NOT_ELIGIBLE'
      using errcode='40001',detail=jsonb_build_object(
        'code','CANDIDATE_LEGACY_PAPER_ORPHAN_NOT_ELIGIBLE',
        'reason','CURRENT_TOKEN_HAS_OWNER',
        'owner_count',v_current_token_owner_count
      )::text;
  end if;

  update public.timesheets timesheet_row
  set qr_token=null,
      qr_payload_json='{}'::jsonb,
      qr_generated_at=null,
      qr_scanned_at=null,
      qr_scan_info_json=null,
      qr_r2_key=null,
      qr_last_sent_hash=null,
      qr_last_sent_at_utc=null,
      qr_signed_hash=null,
      qr_signed_at_utc=null,
      updated_at=v_now_utc
  where timesheet_row.timesheet_id=v_source.timesheet_id
    and timesheet_row.is_current=true
    and timesheet_row.archived_at_utc is null
    and nullif(btrim(coalesce(timesheet_row.qr_token,'')),'') is not null
    and encode(extensions.digest(
      convert_to(timesheet_row.qr_token,'UTF8'),'sha256'
    ),'hex')=v_current_token_hash;
  if not found then
    raise exception 'CANDIDATE_LEGACY_PAPER_ORPHAN_RETIREMENT_LOST_RACE'
      using errcode='40001';
  end if;

  perform private._candidate_audit_v1(
    'candidate_submission_workflow',v_workflow.id::text,
    'CANDIDATE_LEGACY_PAPER_ORPHAN_PREPARED_FOR_CANCELLATION',
    jsonb_build_object(
      'state',v_workflow.state,'generation',v_workflow.generation
    ),
    jsonb_build_object(
      'state',v_workflow.state,'generation',v_workflow.generation,
      'legacy_one_page_pack',true,'orphaned_current_token_retired',true,
      'delivery_receipt_preserved',true
    ),
    'WORKFLOW_CANCELLED',null,
    'candidate-legacy-paper-orphan:'||v_workflow.id::text||':'
      ||v_workflow.generation::text,
    v_now_utc
  );

  return jsonb_build_object(
    'prepared',true,
    'workflow_id',v_workflow.id,
    'generation',v_workflow.generation,
    'legacy_one_page_pack',true,
    'orphaned_current_token_retired',true
  );
end;
$function$;

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
  v_error_message text;
  v_error_detail text;
  v_error_detail_json jsonb;
  v_prepare_result jsonb;
begin
  begin
    return public.candidate_workflow_transition_atomic_v1(
      p_session_id,p_environment,p_workflow_id,'CANCEL',
      p_expected_generation,coalesce(p_payload,'{}'::jsonb),
      p_idempotency_key,p_now_utc
    );
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
  return public.candidate_workflow_transition_atomic_v1(
    p_session_id,p_environment,p_workflow_id,'CANCEL',
    p_expected_generation,coalesce(p_payload,'{}'::jsonb),
    p_idempotency_key,p_now_utc
  );
end;
$function$;

alter function private._candidate_legacy_paper_orphan_prepare_v1(
  text,uuid,integer,timestamptz
) owner to postgres;
alter function public.candidate_workflow_cancel_atomic_v2(
  uuid,text,uuid,integer,jsonb,text,timestamptz
) owner to postgres;

revoke all on function private._candidate_legacy_paper_orphan_prepare_v1(
  text,uuid,integer,timestamptz
) from public,anon,authenticated,service_role;
revoke all on function public.candidate_workflow_cancel_atomic_v2(
  uuid,text,uuid,integer,jsonb,text,timestamptz
) from public,anon,authenticated;
grant execute on function public.candidate_workflow_cancel_atomic_v2(
  uuid,text,uuid,integer,jsonb,text,timestamptz
) to service_role;

notify pgrst, 'reload schema';

commit;
