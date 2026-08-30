-- Candidate printed-pack Manifest V2 and page-specific QR proof authority.
-- V1 stays valid for already-issued packs. New PAPER prepares are promoted by
-- the private Candidate Worker before the held email is released.

\set ON_ERROR_STOP on

begin;

create or replace function public.candidate_paper_manifest_v2_promote_v1(
  p_session_id uuid,
  p_environment text,
  p_workflow_id uuid,
  p_expected_generation integer,
  p_expected_v1_manifest_sha256_hex text,
  p_now_utc timestamptz default now()
)
returns jsonb
language plpgsql
volatile
security definer
set search_path = pg_catalog, public, private, extensions, pg_temp
as $function$
declare
  v_session jsonb;
  v_candidate_id uuid;
  v_workflow public.candidate_submission_workflows%rowtype;
  v_timesheet public.timesheets%rowtype;
  v_old_manifest_sha256_hex text;
  v_new_pages jsonb;
  v_new_manifest jsonb;
  v_new_manifest_sha256 bytea;
  v_new_manifest_sha256_hex text;
  v_outbox_count integer;
  v_response jsonb;
begin
  perform private._candidate_require_feature_v1(p_environment,'candidate_app_writes');
  if p_expected_generation is null or p_expected_generation<1
     or lower(coalesce(p_expected_v1_manifest_sha256_hex,'')) !~ '^[0-9a-f]{64}$' then
    raise exception 'CANDIDATE_PAPER_RETURN_MANIFEST_STALE' using errcode='22023';
  end if;
  v_session:=private._candidate_session_context_v1(
    p_session_id,p_environment,null,p_now_utc,true
  );
  v_candidate_id:=nullif(v_session->>'selected_candidate_id','')::uuid;
  select * into v_workflow
  from public.candidate_submission_workflows
  where id=p_workflow_id and candidate_id=v_candidate_id
  for update;
  if not found then raise exception 'CANDIDATE_WORKFLOW_NOT_FOUND' using errcode='P0002'; end if;
  if v_workflow.generation<>p_expected_generation
     or v_workflow.scope<>'WEEKLY'
     or v_workflow.route<>'PAPER'
     or v_workflow.state<>'AWAITING_PAPER_RETURN'
     or v_workflow.paper_return_manifest_sha256 is null
     or private._candidate_sha256_jsonb_v1(v_workflow.paper_return_manifest_json)
       is distinct from v_workflow.paper_return_manifest_sha256 then
    raise exception 'CANDIDATE_PAPER_RETURN_MANIFEST_STALE' using errcode='40001';
  end if;

  if coalesce((v_workflow.paper_return_manifest_json->>'manifest_version')::integer,1)=2 then
    if v_workflow.paper_return_manifest_json->>'qr_contract_version'
         <>'CANDIDATE_PAPER_PAGE_QR_V2' then
      raise exception 'CANDIDATE_PAPER_RETURN_MANIFEST_STALE' using errcode='40001';
    end if;
    return jsonb_build_object(
      'ok',true,'idempotent_replay',true,'workflow_id',v_workflow.id,
      'generation',v_workflow.generation,'manifest_version',2,
      'qr_contract_version','CANDIDATE_PAPER_PAGE_QR_V2',
      'paper_return_manifest_sha256',encode(v_workflow.paper_return_manifest_sha256,'hex'),
      'paper_return_page_count',jsonb_array_length(v_workflow.paper_return_manifest_json->'pages')
    );
  end if;

  v_old_manifest_sha256_hex:=encode(v_workflow.paper_return_manifest_sha256,'hex');
  if v_old_manifest_sha256_hex<>lower(p_expected_v1_manifest_sha256_hex) then
    raise exception 'CANDIDATE_PAPER_RETURN_MANIFEST_STALE' using errcode='40001';
  end if;
  select * into v_timesheet
  from public.timesheets
  where timesheet_id=coalesce(v_workflow.target_timesheet_id,v_workflow.anchor_timesheet_id)
    and is_current=true and archived_at_utc is null
  for share;
  if not found then raise exception 'CANDIDATE_TIMESHEET_NOT_FOUND' using errcode='P0002'; end if;

  with manifest_pages as (
    select page,ordinality::integer as ordinal,
      count(*) over (
        partition by coalesce(page->>'component_kind',''),coalesce(page->>'expense_category','')
        order by ordinality rows between unbounded preceding and current row
      )::integer as category_occurrence
    from jsonb_array_elements(v_workflow.paper_return_manifest_json->'pages')
      with ordinality source(page,ordinality)
  )
  select jsonb_agg(
    page || jsonb_build_object(
      'ordinal',ordinal,
      'display_name',case page->>'component_kind'
        when 'HOURS_TIMESHEET' then 'Timesheet'
        when 'EXPENSE_SUMMARY' then 'Expense summary'
        when 'MILEAGE_FORM' then 'Mileage form'
        when 'EXPENSE_EVIDENCE' then initcap(lower(page->>'expense_category'))||' '||category_occurrence
        else 'Document '||ordinal end,
      'expense_category',case
        when page->>'component_kind'='EXPENSE_SUMMARY' then 'OTHER'
        else page->>'expense_category' end,
      'category_occurrence',category_occurrence,
      'page_kind_code',case page->>'component_kind'
        when 'HOURS_TIMESHEET' then 'T'
        when 'EXPENSE_SUMMARY' then 'S'
        when 'MILEAGE_FORM' then 'M'
        else 'E' end,
      'category_code',case coalesce(page->>'expense_category','')
        when 'ACCOMMODATION' then 'A'
        when 'MILEAGE' then 'M'
        when 'OTHER' then 'O'
        when 'TRAVEL' then 'T'
        else '' end,
      'page_key_sha256_16',substring(encode(
        extensions.digest(page->>'page_key','sha256'),'hex'
      ) from 1 for 16),
      'qr_required',true
    ) order by ordinal
  ) into v_new_pages
  from manifest_pages;
  if jsonb_typeof(v_new_pages)<>'array' or jsonb_array_length(v_new_pages)<1 then
    raise exception 'CANDIDATE_PAPER_RETURN_MANIFEST_STALE' using errcode='40001';
  end if;

  v_new_manifest:=jsonb_build_object(
    'manifest_version',2,
    'qr_contract_version','CANDIDATE_PAPER_PAGE_QR_V2',
    'workflow_id',v_workflow.id,
    'workflow_generation',v_workflow.generation,
    'timesheet_id',v_timesheet.timesheet_id,
    'timesheet_document_revision',coalesce(v_timesheet.document_revision,0),
    'immutable_submission_sha256',encode(v_workflow.immutable_submission_sha256,'hex'),
    'pages',v_new_pages
  );
  v_new_manifest_sha256:=private._candidate_sha256_jsonb_v1(v_new_manifest);
  v_new_manifest_sha256_hex:=encode(v_new_manifest_sha256,'hex');

  select count(*)::integer into v_outbox_count
  from public.mail_outbox candidate_mail
  where candidate_mail.type='TIMESHEET_QR'
    and candidate_mail.context_kind='timesheets'
    and candidate_mail.context_id=v_timesheet.timesheet_id
    and candidate_mail.status='QUEUED'
    and candidate_mail.attempt_lease_token is null
    and candidate_mail.payment_scope_json->>'candidate_workflow_id'=v_workflow.id::text
    and candidate_mail.payment_scope_json->>'candidate_workflow_generation'=v_workflow.generation::text
    and lower(coalesce(candidate_mail.payment_scope_json->>'paper_return_manifest_sha256',''))
      =v_old_manifest_sha256_hex
    and lower(coalesce(candidate_mail.payment_scope_json->>'candidate_paper_pack_ready','false'))
      in ('false','f','0','no')
    and jsonb_array_length(coalesce(candidate_mail.attachments,'[]'::jsonb))=0;
  if v_outbox_count<>1 then
    raise exception 'CANDIDATE_PAPER_OUTBOX_CONFLICT' using errcode='40001';
  end if;

  update public.mail_outbox candidate_mail set
    payment_scope_json=jsonb_set(
      jsonb_set(candidate_mail.payment_scope_json,
        '{paper_return_manifest_sha256}',to_jsonb(v_new_manifest_sha256_hex),true),
      '{candidate_paper_manifest_version}','2'::jsonb,true
    )
  where candidate_mail.type='TIMESHEET_QR'
    and candidate_mail.context_kind='timesheets'
    and candidate_mail.context_id=v_timesheet.timesheet_id
    and candidate_mail.status='QUEUED'
    and candidate_mail.attempt_lease_token is null
    and candidate_mail.payment_scope_json->>'candidate_workflow_id'=v_workflow.id::text
    and candidate_mail.payment_scope_json->>'candidate_workflow_generation'=v_workflow.generation::text
    and lower(coalesce(candidate_mail.payment_scope_json->>'paper_return_manifest_sha256',''))
      =v_old_manifest_sha256_hex;

  v_response:=coalesce(v_workflow.last_mutation_response_json,'{}'::jsonb)
    ||jsonb_build_object(
      'paper_return_manifest_sha256',v_new_manifest_sha256_hex,
      'paper_return_page_count',jsonb_array_length(v_new_pages),
      'paper_return_manifest_version',2,
      'paper_return_qr_contract_version','CANDIDATE_PAPER_PAGE_QR_V2'
    );
  update public.candidate_submission_workflows set
    paper_return_manifest_json=v_new_manifest,
    paper_return_manifest_sha256=v_new_manifest_sha256,
    last_mutation_response_json=v_response,
    updated_at_utc=p_now_utc
  where id=v_workflow.id;

  return jsonb_build_object(
    'ok',true,'idempotent_replay',false,'workflow_id',v_workflow.id,
    'generation',v_workflow.generation,'manifest_version',2,
    'qr_contract_version','CANDIDATE_PAPER_PAGE_QR_V2',
    'paper_return_manifest_sha256',v_new_manifest_sha256_hex,
    'paper_return_page_count',jsonb_array_length(v_new_pages)
  );
end
$function$;

create or replace function public.candidate_paper_return_proof_validate_v2(
  p_session_id uuid,
  p_environment text,
  p_workflow_id uuid,
  p_expected_generation integer,
  p_manifest_sha256_hex text,
  p_page_key text,
  p_qr_payload jsonb,
  p_now_utc timestamptz default now()
)
returns jsonb
language plpgsql
volatile
security definer
set search_path = pg_catalog, public, private, extensions, pg_temp
as $function$
declare
  v_session jsonb;
  v_candidate_id uuid;
  v_workflow public.candidate_submission_workflows%rowtype;
  v_timesheet public.timesheets%rowtype;
  v_page jsonb;
  v_manifest_hex text;
  v_proof_identity text;
begin
  perform private._candidate_require_feature_v1(p_environment,'candidate_app_writes');
  if p_expected_generation is null or p_expected_generation<1
     or lower(coalesce(p_manifest_sha256_hex,'')) !~ '^[0-9a-f]{64}$'
     or nullif(btrim(coalesce(p_page_key,'')),'') is null
     or jsonb_typeof(p_qr_payload)<>'object'
     or (select array_agg(payload_key order by payload_key)
         from jsonb_object_keys(p_qr_payload) as payload_keys(payload_key))
       is distinct from array['c','g','k','m','n','o','p','t','v','w']::text[] then
    raise exception 'CANDIDATE_PAPER_QR_PROOF_INVALID' using errcode='22023';
  end if;
  v_session:=private._candidate_session_context_v1(
    p_session_id,p_environment,null,p_now_utc,true
  );
  v_candidate_id:=nullif(v_session->>'selected_candidate_id','')::uuid;
  select * into v_workflow
  from public.candidate_submission_workflows
  where id=p_workflow_id and candidate_id=v_candidate_id
  for update;
  if not found then raise exception 'CANDIDATE_WORKFLOW_NOT_FOUND' using errcode='P0002'; end if;
  if v_workflow.generation<>p_expected_generation
     or v_workflow.route<>'PAPER'
     or v_workflow.state<>'AWAITING_PAPER_RETURN'
     or coalesce((v_workflow.paper_return_manifest_json->>'manifest_version')::integer,0)<>2
     or v_workflow.paper_return_manifest_json->>'qr_contract_version'
       <>'CANDIDATE_PAPER_PAGE_QR_V2'
     or v_workflow.paper_return_manifest_sha256 is null
     or private._candidate_sha256_jsonb_v1(v_workflow.paper_return_manifest_json)
       is distinct from v_workflow.paper_return_manifest_sha256 then
    raise exception 'CANDIDATE_PAPER_QR_PROOF_STALE' using errcode='40001';
  end if;
  v_manifest_hex:=encode(v_workflow.paper_return_manifest_sha256,'hex');
  if v_manifest_hex<>lower(p_manifest_sha256_hex) then
    raise exception 'CANDIDATE_PAPER_RETURN_MANIFEST_STALE' using errcode='40001';
  end if;
  select page into v_page
  from jsonb_array_elements(v_workflow.paper_return_manifest_json->'pages') page
  where page->>'page_key'=p_page_key;
  if v_page is null then
    raise exception 'CANDIDATE_PAPER_RETURN_PAGE_NOT_EXPECTED' using errcode='22023';
  end if;
  select * into v_timesheet
  from public.timesheets
  where timesheet_id=coalesce(v_workflow.target_timesheet_id,v_workflow.anchor_timesheet_id)
    and is_current=true and archived_at_utc is null;
  if not found then raise exception 'CANDIDATE_TIMESHEET_NOT_FOUND' using errcode='P0002'; end if;

  if (p_qr_payload->>'v')::integer<>2
     or lower(p_qr_payload->>'w')<>v_workflow.id::text
     or lower(p_qr_payload->>'t')<>v_timesheet.timesheet_id::text
     or (p_qr_payload->>'g')::integer<>v_workflow.generation
     or lower(p_qr_payload->>'m')<>v_manifest_hex
     or (p_qr_payload->>'o')::integer<>(v_page->>'ordinal')::integer
     or lower(p_qr_payload->>'p')<>lower(v_page->>'page_key_sha256_16')
     or p_qr_payload->>'k'<>v_page->>'page_kind_code'
     or coalesce(p_qr_payload->>'c','')<>coalesce(v_page->>'category_code','')
     or (p_qr_payload->>'n')::integer<>(v_page->>'category_occurrence')::integer then
    raise exception 'CANDIDATE_PAPER_QR_PROOF_MISMATCH' using errcode='28000';
  end if;
  v_proof_identity:=concat_ws('|',
    'CANDIDATE_PAPER_RETURN_PROOF_V2',v_workflow.id,v_workflow.generation,
    v_manifest_hex,p_page_key,p_qr_payload::text
  );
  return jsonb_build_object(
    'ok',true,
    'proof_contract_version','CANDIDATE_PAPER_RETURN_PROOF_V2',
    'workflow_id',v_workflow.id,
    'workflow_generation',v_workflow.generation,
    'timesheet_id',v_timesheet.timesheet_id,
    'paper_return_manifest_sha256',v_manifest_hex,
    'paper_return_page_key',p_page_key,
    'page_component_kind',v_page->>'component_kind',
    'qr_required',true,
    'qr_payload_sha256',encode(extensions.digest(p_qr_payload::text,'sha256'),'hex'),
    'proof_receipt_sha256',encode(extensions.digest(v_proof_identity,'sha256'),'hex')
  );
end
$function$;

alter function public.candidate_paper_manifest_v2_promote_v1(
  uuid,text,uuid,integer,text,timestamptz
) owner to postgres;
alter function public.candidate_paper_return_proof_validate_v2(
  uuid,text,uuid,integer,text,text,jsonb,timestamptz
) owner to postgres;

revoke all on function public.candidate_paper_manifest_v2_promote_v1(
  uuid,text,uuid,integer,text,timestamptz
) from public,anon,authenticated;
revoke all on function public.candidate_paper_return_proof_validate_v2(
  uuid,text,uuid,integer,text,text,jsonb,timestamptz
) from public,anon,authenticated;

grant execute on function public.candidate_paper_manifest_v2_promote_v1(
  uuid,text,uuid,integer,text,timestamptz
) to service_role;
grant execute on function public.candidate_paper_return_proof_validate_v2(
  uuid,text,uuid,integer,text,text,jsonb,timestamptz
) to service_role;

comment on function public.candidate_paper_manifest_v2_promote_v1(
  uuid,text,uuid,integer,text,timestamptz
) is 'Promotes a held current Weekly PAPER manifest to page-specific QR V2 before the pack email is released.';
comment on function public.candidate_paper_return_proof_validate_v2(
  uuid,text,uuid,integer,text,text,jsonb,timestamptz
) is 'Validates a signed TSQ2 page identity against the exact current Candidate PAPER manifest. Service-only; raw QR text is not retained.';

notify pgrst, 'reload schema';

commit;
